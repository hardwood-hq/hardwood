# Maven

To run Maven, always run ./mvnw (Maven wrapper).
Run ./mvnw verify to build the project.
When doing changes in hardwood-core, install that module before running the performance tests or any other module.
When running Maven commands, always apply a timeout of 180 seconds to detect deadlocks early on.
Enable -Pperformance-test to run performance tests.

# Design and Coding

Keep cyclomatic complexity low.
Avoid fully-qualified class names within the code, always add imports.
Avoid object access and boxing as much as possible. Always prefer primitive access also if it means several similar methods.
Before writing new code, search for existing patterns in the same class/package that accomplish the same thing (e.g., the DRY principle). Extract repeated logic into helper methods within the same class rather than duplicating it. When a pattern appears multiple times, consider consolidating it into a single well-named method with overloads if needed.
Be conservative with base class refactoring. Do not pull implementation details up into abstract base classes unless the logic is truly identical across all subclasses with no foreseeable divergence. Shared helpers are better than shared template methods when subclasses may need different control flow.

# Testing

To generate test Parquet files, extend simple-datagen.py and run: `source .docker-venv/bin/activate && python simple-datagen.py`
When running Python, use _.docker-venv_ as the venv directory.
