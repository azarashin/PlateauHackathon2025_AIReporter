from langchain.agents import Tool

class Calculator(Tool):
    def __init__(self, gml_dirs: list[dir]):
        super().__init__( 
            name="Calclator", 
            func=self._calculator, 
            description="数学計算に使うツール。例： '2+2'や'3*7'")

    def _calculator(self, expression: str) -> str:
        try:
            return str(eval(expression))
        except Exception as e:
            return f"Error: {e}"
