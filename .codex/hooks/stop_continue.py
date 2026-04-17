#!/usr/bin/env python3

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional


TRANSCRIPT_BYTES = int(os.environ.get("CODEX_STOP_CONTINUE_TRANSCRIPT_BYTES", "24000"))
REVIEW_TIMEOUT = int(os.environ.get("CODEX_STOP_CONTINUE_REVIEW_TIMEOUT", "30"))
REVIEW_MODEL = os.environ.get("CODEX_STOP_CONTINUE_MODEL", "").strip()
REVIEW_REASONING = os.environ.get("CODEX_STOP_CONTINUE_REASONING_EFFORT", "low").strip()
MAX_PASSES = max(1, int(os.environ.get("CODEX_STOP_CONTINUE_MAX_PASSES", "8")))
STATE_DIR = Path(
    os.environ.get("CODEX_STOP_CONTINUE_STATE_DIR", tempfile.gettempdir())
) / "codex-stop-continue"
FALLBACK_CONTINUE_REASON = (
    "Continue the task instead of stopping with optional next steps. "
    "Do the next concrete work now, and only stop if the work is complete "
    "or a real blocker requires user input."
)
OPTIONAL_HANDOFF_PATTERNS = (
    re.compile(r"\blet me know if you(?:'d)? like\b", re.IGNORECASE),
    re.compile(r"\bif you(?:'d)? like,? I can\b", re.IGNORECASE),
    re.compile(r"\bif you want,? I can\b", re.IGNORECASE),
    re.compile(r"\bwant me to\b", re.IGNORECASE),
    re.compile(r"\bnext steps?\b", re.IGNORECASE),
    re.compile(
        r"\bI (?:didn't|did not|haven't|have not) run "
        r"(?:tests|the tests|validation|checks)\b",
        re.IGNORECASE,
    ),
)


def emit(payload: Dict[str, Any]) -> int:
    sys.stdout.write(json.dumps(payload))
    sys.stdout.write("\n")
    return 0


def allow_stop() -> int:
    return emit({})


def block_stop(reason: str) -> int:
    return emit(
        {
            "decision": "block",
            "reason": reason,
        }
    )


def debug(message: str) -> None:
    if os.environ.get("CODEX_STOP_CONTINUE_DEBUG") == "1":
        print(message, file=sys.stderr)


def load_payload() -> Dict[str, Any]:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    return json.loads(raw)


def read_transcript_excerpt(transcript_path: Any) -> str:
    if not isinstance(transcript_path, str) or not transcript_path:
        return ""

    try:
        with open(transcript_path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(size - TRANSCRIPT_BYTES, 0))
            data = handle.read()
    except OSError as exc:
        debug(f"stop_continue: failed to read transcript: {exc}")
        return ""

    text = data.decode("utf-8", errors="ignore").strip()
    if not text:
        return ""

    if len(text) > TRANSCRIPT_BYTES:
        return text[-TRANSCRIPT_BYTES:]
    return text


def state_file(payload: Dict[str, Any]) -> Optional[Path]:
    session_id = payload.get("session_id")
    turn_id = payload.get("turn_id")
    if not isinstance(session_id, str) or not session_id:
        return None
    if not isinstance(turn_id, str) or not turn_id:
        return None
    return STATE_DIR / f"{session_id}-{turn_id}.json"


def load_pass_count(payload: Dict[str, Any]) -> int:
    path = state_file(payload)
    if path is None:
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    count = data.get("count")
    if isinstance(count, int) and count >= 0:
        return count
    return 0


def save_pass_count(payload: Dict[str, Any], count: int) -> None:
    path = state_file(payload)
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"count": count}), encoding="utf-8")
    except OSError as exc:
        debug(f"stop_continue: failed to persist pass count: {exc}")


def clear_pass_count(payload: Dict[str, Any]) -> None:
    path = state_file(payload)
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        debug(f"stop_continue: failed to clear pass count: {exc}")


def fallback_continue_reason(payload: Dict[str, Any]) -> Optional[str]:
    assistant_message = payload.get("last_assistant_message")
    assistant_text = assistant_message if isinstance(assistant_message, str) else ""
    if not assistant_text.strip():
        return None

    for pattern in OPTIONAL_HANDOFF_PATTERNS:
        if pattern.search(assistant_text):
            return FALLBACK_CONTINUE_REASON

    return None


def build_review_prompt(
    payload: Dict[str, Any], transcript_excerpt: str, prior_passes: int
) -> str:
    assistant_message = payload.get("last_assistant_message")
    assistant_text = assistant_message if isinstance(assistant_message, str) else ""
    cwd = payload.get("cwd")
    cwd_text = cwd if isinstance(cwd, str) else ""
    stop_hook_active = payload.get("stop_hook_active") is True

    return f"""You are deciding whether Codex should automatically continue working after a turn ended.

Return JSON only, matching the provided schema:
- continue: true if there is any concrete, useful work left that Codex can do now without asking the user for clarification, approval, credentials, or other intervention.
- reason: if continue is true, write a single imperative continuation prompt for Codex. If continue is false, return an empty string.

Bias strongly toward continuing:
- Continue whenever there is unfinished implementation, debugging, validation, cleanup required by the task, or any clear next step the assistant already identified.
- If the assistant stopped with handoff language like "if you want, I can..." or "let me know if you'd like...", treat that as a strong signal to continue.
- Prefer follow-through over handoff when the assistant appears to have stopped early.
- If uncertain between stop and continue, continue.

Stop only when one of these is true:
- The requested work is actually complete and there is no meaningful work left besides optional polish or speculative ideas.
- There is a real blocker that requires the user, explicit approval, credentials, missing external access, or a risky/destructive action.
- The agent is stuck in a loop and there is no better next action than surfacing the blocker.

Hook state:
- stop_hook_active: {str(stop_hook_active).lower()}
- prior automatic continuations in this turn: {prior_passes}
- maximum allowed automatic continuations in this turn: {MAX_PASSES}

Session cwd:
{cwd_text}

Latest assistant message:
<<<ASSISTANT
{assistant_text}
ASSISTANT

Recent transcript excerpt:
<<<TRANSCRIPT
{transcript_excerpt}
TRANSCRIPT
"""


def run_reviewer(payload: Dict[str, Any], prior_passes: int) -> Optional[Dict[str, Any]]:
    cwd = payload.get("cwd")
    workdir = cwd if isinstance(cwd, str) and cwd else os.getcwd()
    transcript_excerpt = read_transcript_excerpt(payload.get("transcript_path"))
    prompt = build_review_prompt(payload, transcript_excerpt, prior_passes)
    schema_path = Path(__file__).with_name("stop_continue_response.schema.json")

    env = os.environ.copy()
    env["CODEX_STOP_CONTINUE_CHILD"] = "1"

    cmd = [
        "codex",
        "exec",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--color",
        "never",
        "--output-schema",
        str(schema_path),
        "-c",
        'features.codex_hooks=false',
        "-c",
        f'model_reasoning_effort="{REVIEW_REASONING or "low"}"',
        "-C",
        workdir,
    ]
    if REVIEW_MODEL:
        cmd.extend(["-m", REVIEW_MODEL])

    with tempfile.TemporaryDirectory(prefix="codex-stop-continue-") as temp_dir:
        output_path = Path(temp_dir) / "review.json"
        cmd.extend(["-o", str(output_path), "-"])

        try:
            result = subprocess.run(
                cmd,
                input=prompt,
                text=True,
                capture_output=True,
                env=env,
                timeout=REVIEW_TIMEOUT,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            debug(f"stop_continue: reviewer launch failed: {exc}")
            return None

        if result.returncode != 0:
            debug(
                "stop_continue: reviewer returned non-zero exit status "
                f"{result.returncode}: {result.stderr.strip()}"
            )
            return None

        try:
            return json.loads(output_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            debug(f"stop_continue: failed to parse reviewer output: {exc}")
            return None


def main() -> int:
    try:
        payload = load_payload()
    except json.JSONDecodeError as exc:
        debug(f"stop_continue: invalid hook payload: {exc}")
        return allow_stop()

    if payload.get("hook_event_name") != "Stop":
        return allow_stop()

    prior_passes = load_pass_count(payload)
    if prior_passes >= MAX_PASSES:
        debug("stop_continue: max continuation passes reached")
        clear_pass_count(payload)
        return allow_stop()

    review = run_reviewer(payload, prior_passes)
    if not isinstance(review, dict):
        fallback_reason = fallback_continue_reason(payload)
        if fallback_reason:
            save_pass_count(payload, prior_passes + 1)
            return block_stop(fallback_reason)
        clear_pass_count(payload)
        return allow_stop()

    should_continue = review.get("continue") is True
    reason = review.get("reason")
    reason_text = reason.strip() if isinstance(reason, str) else ""

    if should_continue and not reason_text:
        reason_text = fallback_continue_reason(payload) or FALLBACK_CONTINUE_REASON

    if not should_continue or not reason_text:
        clear_pass_count(payload)
        return allow_stop()

    save_pass_count(payload, prior_passes + 1)
    return block_stop(reason_text)


if __name__ == "__main__":
    raise SystemExit(main())
