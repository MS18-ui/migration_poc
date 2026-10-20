import re
from typing import Any, List, Optional

from .llm import BaseLLM, ToolResult
from .tools import ToolRegistry


def _extract_artifact(text: str) -> str:
    """The largest fenced code block if the reply has one, else the whole reply."""
    blocks = re.findall(r"```[\w+-]*\n(.*?)```", text, re.S)
    return max(blocks, key=len) if blocks else text


class ToolAgent:
    """
    Observe -> act -> observe loop, provider-agnostic. The model chooses tools
    (validators), reads results, and repairs its output until a tool sets
    `self.final` (via submit_final) or max_turns is reached. History is its memory.
    """

    def __init__(self, name: str, llm: BaseLLM, system: str,
                 tools: ToolRegistry, max_turns: int = 10, verbose: bool = True):
        self.name, self.llm, self.system = name, llm, system
        self.tools, self.max_turns, self.verbose = tools, max_turns, verbose
        self.history: List[dict] = []
        self.final: Optional[Any] = None
        self.abort: Optional[str] = None  # a tool may set this to stop the loop with a reason (e.g. correction limit)
        self.auto_submit = False          # generation agents: a plain-text artifact reply is routed through submit_final

    def _log(self, msg: str):
        if self.verbose:
            print(f"   [{self.name}] {msg}")

    def run(self, task: str) -> Any:
        self.history = [self.llm.user_message(task)]
        self.final = None
        self.abort = None
        for turn in range(1, self.max_turns + 1):
            resp = self.llm.chat(self.system, self.history, self.tools.api_specs())
            self.history.append(self.llm.assistant_message(resp))

            if not resp.tool_calls:
                if self.auto_submit and resp.text.strip():
                    # The model answered with the artifact as plain text. Route it through the SAME validators
                    # (submit_final re-checks everything); it is never accepted unvalidated.
                    output, is_error = self.tools.dispatch("submit_final", {"content": _extract_artifact(resp.text)})
                    self._log(f"turn {turn}: text reply submitted -> {'REJECTED: ' + output[:160] if is_error else 'ok'}")
                    if self.final is not None:
                        self._log(f"accepted on turn {turn}")
                        return self.final
                    if self.abort:
                        raise RuntimeError(f"{self.name} stopped: {self.abort}")
                    self.history.append(self.llm.user_message(
                        "Your text reply was submitted as the final artifact and was not accepted:\n" + output[:1500]
                        + "\nFix it and call the tools (check, then submit_final)."))
                    continue
                self._log(f"turn {turn}: no tool call; nudging to validate and submit")
                self.history.append(self.llm.user_message(
                    "Validate your work with the tools, then call submit_final."))
                continue

            results = []
            for call in resp.tool_calls:
                output, is_error = self.tools.dispatch(call.name, call.args)
                self._log(f"turn {turn}: {call.name} -> {'REJECTED: ' + output[:160] if is_error else 'ok'}")
                results.append(ToolResult(call.id, output, is_error))
            self.history.extend(self.llm.tool_result_messages(results))

            if self.final is not None:
                self._log(f"accepted on turn {turn}")
                return self.final
            if self.abort:
                raise RuntimeError(f"{self.name} stopped: {self.abort}")

        raise RuntimeError(f"{self.name} produced no valid result in {self.max_turns} turns.")
