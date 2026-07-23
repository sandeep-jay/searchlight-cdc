.PHONY: help build test-unit apply-sql sam-test sam-test-all consume-sqs consume-sqs-dry-run replay check

SAM_EVENTS := notes-create notes-update notes-delete note_topics-create note_topics-update note_topics-delete
SAM := LOCAL_DEV=true sam local invoke CDCHandler --env-vars env.json

help:
	@echo "Direct-table CDC production pack"
	@echo "  make apply-sql           Apply sql/*.sql"
	@echo "  make test-unit           pytest test/unit/"
	@echo "  make sam-test            SAM invoke notes-create"
	@echo "  make sam-test-all        SAM invoke all 6 examples"
	@echo "  make replay              Replay events/examples through handler"
	@echo "  make consume-sqs         Live SQS → handler (never deletes messages)"
	@echo "  make consume-sqs-dry-run Queue access check only"

build:
	sam build

sam-test: build
	$(SAM) --event events/examples/notes-create.json

sam-test-all: build
	@for e in $(SAM_EVENTS); do echo "=== $$e ==="; $(SAM) --event events/examples/$$e.json || exit 1; done

replay:
	python scripts/replay_samples.py --dir events/examples

apply-sql:
	python scripts/apply_sql.py

test-unit:
	PYTHONPATH=lambda pytest test/unit/ -v

consume-sqs:
	python scripts/sqs_consume.py --env-key CDCHandler

consume-sqs-dry-run:
	python scripts/sqs_consume.py --env-key CDCHandler --dry-run --max-messages 5

check:
	ruff check lambda/ test/ scripts/ && ruff format --check lambda/ test/ scripts/
