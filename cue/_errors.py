from __future__ import annotations

###############################################################
# Wrapper for a list of errors (format/syntax) which may be encountered when 
# validating a pipeline
#
class PipelineErrorList():
    def __init__(self) -> None:
        self.items = []

    def __getitem__(self, i) -> Errors.PipelineError:
        return self.items[i]
    
    def __iter__(self):
        return self.items.__iter__()
    
    def add(self, error : Errors.PipelineError) -> None:
        self.items.append(error)

    def empty(self):
        return len(self.items) == 0

# TODO: comment
class Errors():
    # context should be +- 2 lines
    class PipelineError():
        def __init__(self,
            name : str,         # name of error
            info : str          # content of error
                ) -> None:

            self.name = name
            self.info = info
            self.location = "?"
            self.starting_line = "?"
            self.details = "?"
            self.context = "\n\n\n"
            self.suggestion = "Sorry, we don't have any suggestions for a fix"

        def __str__(self) -> str:
            return \
                "=" * 64 + "\n" + \
                self.fit(f"{self.name}Exception: {self.info}", indent=0) + "\n" + \
                self.fit(f"At Line {self.location}: {self.details}", indent=2) + "\n" + \
                self.fit(f"Tip: {self.suggestion}") + "\n" + \
                self.indent_and_style_context() + "\n"

        def indent_and_style_context(self) -> str:
            lines = self.context.split('\n')
            styled_lines = [f"    {i+self.starting_line}\t| {line}"for i, line in enumerate(lines)]
            return "\n".join(styled_lines)

        def fit(self, line, indent=2) -> str:
            limit = 64
            lines = []
            current_line = ""
            for word in line.split():
                if len(word + current_line) > limit:
                    lines.append(" " * indent + current_line)
                    current_line = word + " "
                else:
                    current_line += word + " "

            lines.append(" " * indent + current_line)
            
            return "\n".join(lines) + "\n"

        def find_source(self, 
            offending_input : str,
            plaintext : str
                ) -> tuple:
            lines = plaintext.split('\n')
            for i, line in enumerate(lines):
                if offending_input in line:
                    start = i - 2 if i - 2 >= 0 else 0
                    end = i + 3 if i + 3 <= len(lines) else len(lines)

                    return "\n".join(lines[start : end]), start + 1, i + 1

    # TODO: comment
    class ScriptNameCollision(PipelineError):
        def __init__(self, 
            offending_input : str,
            plaintext : str
                ) -> None:
            
            super().__init__("DuplicateScriptName", "script name already in use")
            self.details = f"script names must be unique identifiers, but '{offending_input}'" \
                " is used elsewhere"
            self.suggestion = f"Consider adding a numeric suffix to distinguish between scripts with" \
                " the same name."
            self.context, self.starting_line, self.location = \
                self.find_source(offending_input, plaintext)


    class ScriptInput(PipelineError):
        def __init__(self, 
            offending_input : str,
            suggestion : str, 
            plaintext : str,
                ) -> None:

            super().__init__("ScriptInput", "cannot resolve script input")
            self.offending_input = offending_input
            self.details = f"'{offending_input}' could not be resolved as the output of another script"
            self.suggestion = f"Did you mean '{suggestion}' instead of '{offending_input}'?"
            self.context, self.starting_line, self.location = \
                self.find_source(offending_input, plaintext)

    class ModuleLoadException(PipelineError):
        def __init__(self,
            offending_input : str,
            suggestion : str,
            plaintext : str,
                ) -> None:

            super().__init__("ModuleLoad", "cannot load module")
            self.offending_input = offending_input
            self.details = f"could not load '{offending_input}' as a python module"
            self.suggestion = f"Make sure the import path is correct and ensure that " \
                f" '{offending_input}' exists and that {suggestion} is the right directory."
            self.context, self.starting_line, self.location = \
                self.find_source(offending_input, plaintext)
    
    class ModuleFormatException(PipelineError):
        def __init__(self,
            offending_input : str,
            plaintext : str
                ) -> None:

            super().__init__("ModuleFormat", "cannot interpret module code")
            self.offending_input = offending_input
            self.details = f"could not find the either the run method or the parameters" \
                f" field in '{offending_input}"
            self.suggestion = f"ensure that '{offending_input}' has a list 'parameters = [...]' " \
                f" and a 'def run(params, data):' method." 
            self. context, self.starting_line, self.location = \
                self.find_source(offending_input, plaintext)
