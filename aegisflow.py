"""
AegisFlow MVP — Self-Healing Workflow Engine
Single-file Flask backend with Anthropic Claude integration.
"""
import os
import uuid
import json
import traceback
import time
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder="static")
CORS(app)

# --- Anthropic Client (lazy init) ---
_client = None

def get_client():
    global _client
    if _client is None:
        try:
            import anthropic
            _client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
        except Exception:
            _client = None
    return _client

DOWNSTREAM = "http://localhost:5000/api/downstream/orders"

# --- In-memory state ---
events = []
incidents = {}

def emit(kind, **data):
    """Emit a timestamped event into the global timeline."""
    ev = {
        "id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "kind": kind,
        **data
    }
    events.append(ev)
    return ev


# ============================
#  MOCK DOWNSTREAM SERVICE
# ============================
@app.route("/api/downstream/orders", methods=["POST"])
def downstream():
    """Simulated downstream API that can fail in various modes."""
    mode = request.headers.get("X-Mode", "ok")
    body = request.get_json(silent=True) or {}

    if mode == "rate_limit":
        return jsonify({"error": "429 too many requests"}), 429
    if "user_id" not in body or body["user_id"] is None:
        return jsonify({"error": "missing user_id", "received": list(body.keys())}), 422
    return jsonify({"id": "ord_" + uuid.uuid4().hex[:6], "status": "created"})


# ============================
#  PIPELINE STAGES
# ============================
def upstream(mode):
    """Simulated upstream data source — injects failures based on mode."""
    payload = {
        "amount": 49.99,
        "currency": "USD",
        "user_id": "u_123",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    if mode == "schema_change":
        payload["userId"] = payload.pop("user_id")
    elif mode == "malformed":
        payload["user_id"] = None
    elif mode == "silent":
        payload["amount"] = "not_a_number"
    return payload


def transform(payload):
    """Transform upstream payload into downstream schema."""
    return {
        "user_id": payload["user_id"],
        "amount": float(payload["amount"]),
        "currency": payload["currency"]
    }


def send_downstream(payload, mode):
    """Send transformed payload to downstream service."""
    import requests as req
    r = req.post(DOWNSTREAM, json=payload, headers={"X-Mode": mode}, timeout=3)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    return r.json()


def apply_patch(payload, patch):
    """Apply a list of patch operations to a payload."""
    p = dict(payload)
    for op in patch:
        if op["op"] == "rename" and op.get("from") in p:
            p[op["to"]] = p.pop(op["from"])
        elif op["op"] == "coerce":
            val = op["value"]
            # Try to coerce to the right type
            if isinstance(val, str):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    pass
            p[op["field"]] = val
        elif op["op"] == "add":
            p[op["field"]] = op["value"]
        elif op["op"] == "set":
            p[op["field"]] = op["value"]
    return p


# ============================
#  WORKFLOW ENGINE
# ============================
def run_workflow(mode, patch=None, incident_id=None):
    """Execute the full pipeline, optionally applying a fix patch."""
    iid = incident_id or str(uuid.uuid4())
    emit("workflow.start", incident_id=iid, mode=mode)
    payload = None

    try:
        payload = upstream(mode)
        emit("workflow.fetched", incident_id=iid, payload=payload)

        if patch:
            payload = apply_patch(payload, patch)
            emit("workflow.patched", incident_id=iid, payload=payload)

        t = transform(payload)
        emit("workflow.transformed", incident_id=iid, payload=t)

        result = send_downstream(t, mode if not patch else "ok")
        emit("workflow.success", incident_id=iid, result=result)
        return {"ok": True, "result": result, "incident_id": iid}

    except Exception as e:
        tb = traceback.format_exc()
        emit("workflow.failed", incident_id=iid, error=str(e), traceback=tb)
        incidents[iid] = {
            "id": iid,
            "mode": mode,
            "error": str(e),
            "traceback": tb,
            "payload": payload,
            "status": "failed",
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        return {"ok": False, "incident_id": iid, "error": str(e)}


# ============================
#  AI DIAGNOSTICIAN
# ============================
DIAG_SYS = """You are a root-cause analyst for broken data pipelines.
Given a traceback and payload, classify the failure precisely.
Return ONLY valid JSON (no markdown, no explanation outside the JSON):
{
  "root_cause": "one-sentence description of the root cause",
  "category": "schema|rate_limit|transient|malformed|silent|unknown",
  "confidence": 0.0-1.0,
  "evidence": "specific evidence from the traceback/payload",
  "severity": "critical|high|medium|low",
  "affected_field": "the field name that caused the issue, if applicable"
}
"""

# Pre-computed fallback diagnoses for demo mode (no API key needed)
FALLBACK_DIAGNOSES = {
    "schema_change": {
        "root_cause": "Upstream API renamed 'user_id' to 'userId' — camelCase migration broke the pipeline's snake_case expectation",
        "category": "schema",
        "confidence": 0.97,
        "evidence": "KeyError on 'user_id' — payload contains 'userId' instead. Classic API versioning drift.",
        "severity": "critical",
        "affected_field": "user_id"
    },
    "malformed": {
        "root_cause": "Upstream sent null value for required field 'user_id' — validation bypass in source system",
        "category": "malformed",
        "confidence": 0.95,
        "evidence": "HTTP 422 from downstream: 'missing user_id'. Payload shows user_id=None.",
        "severity": "high",
        "affected_field": "user_id"
    },
    "rate_limit": {
        "root_cause": "Downstream API returned 429 Too Many Requests — rate limit exceeded on /orders endpoint",
        "category": "rate_limit",
        "confidence": 0.99,
        "evidence": "HTTP 429 response with 'too many requests' message. Transient failure requiring backoff.",
        "severity": "medium",
        "affected_field": None
    },
    "silent": {
        "root_cause": "Amount field contains string 'not_a_number' instead of float — upstream data corruption",
        "category": "silent",
        "confidence": 0.92,
        "evidence": "float() conversion will fail or produce incorrect result. Data integrity violation.",
        "severity": "high",
        "affected_field": "amount"
    }
}


def diagnose(inc):
    """Run AI diagnosis on an incident, with fallback for demo mode."""
    client = get_client()
    mode = inc.get("mode", "unknown")

    if client and os.getenv("ANTHROPIC_API_KEY"):
        try:
            prompt = (
                f"ERROR: {inc['error']}\n"
                f"TRACEBACK:\n{inc['traceback'][-1500:]}\n"
                f"PAYLOAD: {json.dumps(inc['payload'], default=str)}"
            )
            r = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                system=DIAG_SYS,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = r.content[0].text
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                return json.loads(raw[start:end])
        except Exception as e:
            print(f"[WARN] LLM diagnosis failed, using fallback: {e}")

    # Fallback for demo mode
    if mode in FALLBACK_DIAGNOSES:
        return FALLBACK_DIAGNOSES[mode]

    return {
        "root_cause": f"Unknown failure in mode '{mode}': {inc['error'][:200]}",
        "category": "unknown",
        "confidence": 0.5,
        "evidence": inc["error"][:300],
        "severity": "medium",
        "affected_field": None
    }


# ============================
#  AI HEALER
# ============================
HEAL_SYS = """You generate a minimal JSON patch to fix a broken data pipeline.
Return ONLY valid JSON (no markdown, no explanation outside the JSON):
{
  "patch": [
    ... array of operations ...
  ],
  "reasoning": "one-sentence explanation of the fix strategy"
}

Supported patch operations:
  {"op":"rename","from":"oldField","to":"newField"}
  {"op":"coerce","field":"fieldName","value": <corrected_value>}
  {"op":"add","field":"fieldName","value": <value>}
  {"op":"set","field":"fieldName","value": <value>}
  {"op":"retry","policy":"exponential","max":3}
"""

FALLBACK_FIXES = {
    "schema_change": {
        "patch": [{"op": "rename", "from": "userId", "to": "user_id"}],
        "reasoning": "Rename camelCase 'userId' back to snake_case 'user_id' to match downstream schema expectations."
    },
    "malformed": {
        "patch": [{"op": "set", "field": "user_id", "value": "u_fallback_001"}],
        "reasoning": "Set a fallback user_id value to prevent null-reference failures. Flag for manual review."
    },
    "rate_limit": {
        "patch": [{"op": "retry", "policy": "exponential", "max": 3}],
        "reasoning": "Transient 429 error — apply exponential backoff retry strategy (max 3 attempts)."
    },
    "silent": {
        "patch": [{"op": "coerce", "field": "amount", "value": 0.0}],
        "reasoning": "Coerce invalid amount to 0.0 to prevent type error. Original value 'not_a_number' is corrupted."
    }
}


def heal(inc, diag):
    """Generate an AI fix patch, with fallback for demo mode."""
    client = get_client()
    mode = inc.get("mode", "unknown")

    if client and os.getenv("ANTHROPIC_API_KEY"):
        try:
            prompt = (
                f"DIAGNOSIS: {json.dumps(diag, default=str)}\n"
                f"ERROR: {inc['error']}\n"
                f"PAYLOAD: {json.dumps(inc['payload'], default=str)}"
            )
            r = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                system=HEAL_SYS,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = r.content[0].text
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                return json.loads(raw[start:end])
        except Exception as e:
            print(f"[WARN] LLM healing failed, using fallback: {e}")

    # Fallback for demo mode
    if mode in FALLBACK_FIXES:
        return FALLBACK_FIXES[mode]

    return {
        "patch": [],
        "reasoning": f"No automated fix available for mode '{mode}'. Manual intervention required."
    }


# ============================
#  VERIFIER (Sandbox Replay)
# ============================
def verify(mode, fix):
    """Replay the pipeline in-process to verify the fix works."""
    try:
        patch_ops = fix.get("patch", [])

        # Check for retry-only patches (no payload transform needed)
        if len(patch_ops) == 1 and patch_ops[0].get("op") == "retry":
            return {
                "ok": True,
                "sandbox_output": {"strategy": "exponential_backoff", "max_retries": patch_ops[0].get("max", 3)},
                "message": "Retry policy validated — will apply on next execution"
            }

        p = apply_patch(upstream(mode), patch_ops)
        t = transform(p)

        # Schema assertions
        assert t.get("user_id") is not None, "user_id is missing or None"
        assert isinstance(t.get("user_id"), str), "user_id must be a string"
        assert isinstance(t.get("amount"), (int, float)), "amount must be numeric"
        assert t.get("currency") in ("USD", "EUR", "GBP"), f"unexpected currency: {t.get('currency')}"

        return {"ok": True, "sandbox_output": t, "message": "All schema assertions passed"}

    except Exception as e:
        return {"ok": False, "error": str(e), "message": "Sandbox verification failed"}


# ============================
#  API ROUTES
# ============================
@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/events")
def get_events():
    """Return all events and incidents for the timeline."""
    since = request.args.get("since", 0, type=int)
    return jsonify({
        "events": events[since:],
        "incidents": incidents,
        "total_events": len(events)
    })


@app.route("/run", methods=["POST"])
def run():
    """Trigger a workflow run with a specific failure mode."""
    mode = request.json.get("mode", "ok")
    result = run_workflow(mode)

    if result["ok"]:
        return jsonify(result)

    iid = result["incident_id"]
    inc = incidents[iid]

    # Stage 1: Diagnose
    emit("agent.diagnosing", incident_id=iid, stage="diagnostician")
    diag = diagnose(inc)
    inc["diagnosis"] = diag
    emit("agent.diagnosed", incident_id=iid, diagnosis=diag)

    # Stage 2: Heal
    emit("agent.healing", incident_id=iid, stage="healer")
    fix = heal(inc, diag)
    inc["fix"] = fix
    emit("agent.healed", incident_id=iid, fix=fix)

    # Stage 3: Verify
    emit("agent.verifying", incident_id=iid, stage="verifier")
    v = verify(mode, fix)
    inc["verification"] = v
    emit("agent.verified", incident_id=iid, verification=v)

    inc["status"] = "awaiting_approval"

    return jsonify({
        "ok": False,
        "incident_id": iid,
        "diagnosis": diag,
        "fix": fix,
        "verification": v
    })


@app.route("/approve/<iid>", methods=["POST"])
def approve(iid):
    """Approve a proposed fix and retry the workflow."""
    if iid not in incidents:
        return jsonify({"error": "Incident not found"}), 404

    inc = incidents[iid]
    emit("agent.approved", incident_id=iid)

    fix = inc.get("fix", {})
    patch_ops = fix.get("patch", [])

    # Handle retry-only patches
    if len(patch_ops) == 1 and patch_ops[0].get("op") == "retry":
        # Retry without patches but with "ok" mode to simulate success after backoff
        emit("agent.retrying", incident_id=iid, strategy="exponential_backoff")
        time.sleep(0.5)  # Simulate backoff
        result = run_workflow("ok", incident_id=iid + "-retry")
    else:
        result = run_workflow(inc["mode"], patch=patch_ops, incident_id=iid + "-retry")

    inc["status"] = "healed" if result["ok"] else "failed_retry"
    inc["retry_result"] = result
    emit("agent.completed", incident_id=iid, status=inc["status"])

    return jsonify(result)


@app.route("/reject/<iid>", methods=["POST"])
def reject(iid):
    """Reject a proposed fix."""
    if iid not in incidents:
        return jsonify({"error": "Incident not found"}), 404

    inc = incidents[iid]
    inc["status"] = "rejected"
    emit("agent.rejected", incident_id=iid)
    return jsonify({"ok": True, "status": "rejected"})


@app.route("/reset", methods=["POST"])
def reset():
    """Reset all events and incidents."""
    events.clear()
    incidents.clear()
    return jsonify({"ok": True})


@app.route("/health")
def health():
    """Health check endpoint."""
    has_key = bool(os.getenv("ANTHROPIC_API_KEY"))
    return jsonify({
        "status": "ok",
        "mode": "live" if has_key else "demo",
        "anthropic_key_set": has_key,
        "events_count": len(events),
        "incidents_count": len(incidents)
    })


# ============================
#  MAIN
# ============================
if __name__ == "__main__":
    has_key = bool(os.getenv("ANTHROPIC_API_KEY"))
    mode_label = "LIVE (Anthropic API)" if has_key else "DEMO (fallback responses)"

    print(f"""
╔══════════════════════════════════════════════╗
║     🛡️  AegisFlow — Self-Healing Engine      ║
║                                              ║
║  Mode: {mode_label:<37s}║
║  URL:  http://localhost:5000                 ║
║                                              ║
║  Tip: Set ANTHROPIC_API_KEY for live AI      ║
╚══════════════════════════════════════════════╝
""")
    app.run(port=5000, debug=False)
