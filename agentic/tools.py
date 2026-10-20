"""Tool-calling layer: define tools once, expose them to the LLM, dispatch calls."""
from dataclasses import dataclass
from typing import Callable, Dict, List

TEXT_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "content": {"type": "string", "description": "Full artifact text, no markdown fences."}
    },
    "required": ["content"],
}


@dataclass
class ToolSpec:
    name: str
    description: str
    fn: Callable[[dict], str]  # returns "OK: ..." or "ERROR: ..."
    input_schema: dict = None

    def __post_init__(self):
        if self.input_schema is None:
            self.input_schema = TEXT_INPUT_SCHEMA

    def to_api(self) -> dict:
        return {"name": self.name, "description": self.description, "input_schema": self.input_schema}


class ToolRegistry:
    def __init__(self, tools: List[ToolSpec] = ()):
        self._tools: Dict[str, ToolSpec] = {}
        for t in tools:
            self.register(t)

    def register(self, tool: ToolSpec) -> None:
        self._tools[tool.name] = tool

    def api_specs(self) -> List[dict]:
        return [t.to_api() for t in self._tools.values()]

    def dispatch(self, name: str, args: dict) -> tuple:
        """Run a tool. Returns (output, is_error). Crashes are returned, never raised."""
        tool = self._tools.get(name)
        if tool is None:
            return f"ERROR: unknown tool '{name}'", True
        missing = [k for k in tool.input_schema.get("required", []) if not (args or {}).get(k)]
        if missing:  # empty/truncated tool call: tell the model exactly what to resend
            return (f"ERROR: tool '{name}' called without required argument(s) {missing}. "
                    "Resend the call with the FULL artifact text in 'content'; if it was cut off, "
                    "make the artifact more compact."), True
        try:
            out = tool.fn(args)
        except Exception as exc:
            out = f"ERROR: {type(exc).__name__}: {exc}"
        return out, out.startswith("ERROR")
