from typing import Any, List, Optional

from .llm import BaseLLM, ToolResult
from .tools import ToolRegistry


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

    def _log(self, msg: str):
        if self.verbose:
            print(f"   [{self.name}] {msg}")

    def run(self, task: str) -> Any:
        self.history = [self.llm.user_message(task)]
        self.final = None
        for turn in range(1, self.max_turns + 1):
            resp = self.llm.chat(self.system, self.history, self.tools.api_specs())
            self.history.append(self.llm.assistant_message(resp))

            if not resp.tool_calls:
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

        raise RuntimeError(f"{self.name} produced no valid result in {self.max_turns} turns.")
