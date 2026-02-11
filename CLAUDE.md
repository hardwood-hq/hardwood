When running Python, use _.docker-venv_ as the venv directory.
To run Maven, always run ./mvnw (Maven wrapper).
Run ./mvnw verify to build the project.
Keep cyclomatic complexity low.
Avoid fully-qualified class names within the code, always add imports.
Enable -Pperformance-test to run performance tests.
Avoid object access and boxing as much as possible. Always prefer primitive access also if it means several similar methods.
To generate test Parquet files, extend simple-datagen.py and run: `source .docker-venv/bin/activate && python simple-datagen.py`
