# Example CDC events (committed)

Synthetic SQS Lambda envelopes for local SAM testing. **Not real student or advisor data.**

| File | Table | Effective operation |
|------|-------|---------------------|
| `notes-create.json` | `notes` | create |
| `notes-update.json` | `notes` | update |
| `notes-delete.json` | `notes` | delete (soft: `deleted_at` set) |
| `note_topics-create.json` | `note_topics` | create |
| `note_topics-update.json` | `note_topics` | update |
| `note_topics-delete.json` | `note_topics` | delete (soft: `deleted_at` set) |

All examples share synthetic IDs (`sid` `9000000000001`, note `900001`, topic `900101`). Note bodies and subjects use lorem ipsum placeholder text — not real advising content.

```bash
make sam-test       # notes-create only
make sam-test-all   # all six event types
```

Replay without SAM:

```bash
make replay
```

Captured live queue samples belong in a gitignored local `data/` folder. See [local development](../../docs/local-development.md).
