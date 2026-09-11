---
name: simple-english
description: |
  Write or rewrite text in plain, layman-readable English in the spirit of
  ASD-STE100 Simplified Technical English: short sentences, active voice,
  simple tenses, one word one meaning, condition before command, every
  technical term defined at first use, no AI slop. Default mode is Plain.
  Strict mode applies full STE vocabulary compliance when the user names
  STE, ASD-STE100, or compliance. Use for documentation, READMEs, runbooks,
  procedures, error messages, release notes, incident reports, API guides,
  and explanations for readers outside the field. Also use when the user
  says "STE", "Simplified Technical English", "ASD-STE100", "plain English",
  "layman's terms", "explain it simply", "no jargon", "de-slop", "make this
  readable", "write for non-native readers", or asks for docs that translate
  well. The same rules govern the reply: answer first, five sentences or
  fewer, prose only.
license: MIT
compatibility: claude-code cursor codex gemini-cli opencode
metadata:
  version: "2.0.1"
  standard: ASD-STE100 Issue 9 (2025-01-15)
---

# Simple English

Write plain English that a smart reader outside your field understands on one read. The rules come from ASD-STE100, the controlled language aerospace uses so a tired mechanic cannot misread an instruction. Two registers exist: the document you write or rewrite, and the reply you type in chat. Each has its own short rule set below. Nothing else in this file is optional.

## The Document

When asked to write or rewrite documentation, apply these rules to the prose:

1. **Classify each passage.** Procedural text tells the reader what to do: imperative mood, 20 words per sentence, one instruction per sentence. Descriptive text explains: simple tenses, 25 words per sentence, one topic per paragraph, six sentences per paragraph at most.
2. **Condition before command, with a comma.** "If the build fails, read the log."
3. **Simple tenses, active voice.** No present perfect ("has completed" → "completed"). No "-ing" verb after a comma (", making it easy" → new sentence). Name the actor: "You run the migration."
4. **Modals: can, will, must.** Never should, would, may, might, could. A required "should" becomes "must". An optional one is deleted.
5. **Complete grammar.** No contractions, keep articles, keep "that". Short sentences, not telegraph style.
6. **No semicolons and no em-dashes.** Write two sentences, or name the relation.
7. **One word, one meaning, for the whole document.** Use `make sure that` for check, verify, confirm, validate, ensure. Use `configuration` for config, settings, options. Break noun chains over three words with a preposition ("the timeout value for the connection pool").
8. **Define a concept term at its first use**, under ten words, one per sentence. Do not define product names, standard names (Postgres, S3, HTTP), or the tool the document is about.
9. **State the fact, not its importance.** Delete words that carry no fact: simply, seamlessly, robust, powerful, comprehensive, leverage, crucial, "in order to", "it is worth noting". No "not just X, it is Y". No decorative triplets. No "in conclusion".
10. **Format for the eye, not for decoration.** No bold lead-ins, no bold as emphasis, no emoji, no heading over two sentences. A vertical list is for three or more parallel items or steps: colon on the lead-in, uppercase start, one instruction per item.
11. **Warnings: command or condition first, then the risk.** "Do not run this against production. The command deletes rows."
12. **Never touch** code, identifiers, commands, flags, file paths, quoted errors, product names, or facts. When the source gives no number or cause, keep the general statement.

Use American spelling. `references/word-swaps.md` maps the overused words to plain ones.

**Before (real AI output):**

> **Connection timeouts.** If sqlpipe hangs or fails with `dial tcp: i/o timeout`, check that the host running sqlpipe can reach the Postgres port (usually 5432) — this is often a security group or firewall rule blocking the connection. If you're connecting to a managed database (RDS, Cloud SQL, etc.), confirm the instance allows connections from sqlpipe's IP.

**After (procedural, headed, numbered):**

> ## Connection timeouts
>
> sqlpipe stops with `dial tcp: i/o timeout` when it cannot connect to the Postgres port (5432 by default).
>
> 1. Make sure that the host that runs sqlpipe can connect to the Postgres port. A firewall or security group usually blocks it.
> 2. If the database is managed (RDS, Cloud SQL), make sure that the instance accepts connections from the IP of sqlpipe.

## The Reply

Every chat reply, in every mode, follows these rules. Read them last, apply them first:

1. Answer in prose. No headers, no bullet lists, no bold, no tables. A code block is legal when the reader must copy it.
2. Five sentences maximum. Every sentence counts, list items and captions included. Count them before you send. Over five, delete sentences until five remain.
3. The first sentence gives the answer or the result. Do not restate the question.
4. No em-dashes. Name the relation ("because", "but", "for example") or write two sentences.
5. Define a concept term in a few words the first time you use it: "idempotent (safe to run twice)". Do not define product names.
6. No contractions. No openers ("Certainly", "Great question") and no closers ("I hope this helps", "Let me know").
7. Do not shorten quoted error text, security warnings, or confirmations before a destructive action.

**Before:** The failure stems from control-plane leader election during pod churn — nothing to worry about!
**After:** The pods restarted and the queue lost its leader for a short time. It recovered without help. You do not have to do anything.

## Self-Check Before You Deliver

1. Reply: count the sentences. Over five, cut. Search for `—`, `**`, `#`, and a line that starts with `-`. Remove each one.
2. Document: count the words in your three longest sentences. Over 20 or 25, split. Search for `'`, `has been`, `should`, `may`, `;`, `—`, `, making`, `check`, `verify`, `config`. Fix each hit.

## Modes

**Plain** is the default and is all of the above. **Strict** applies when the user names STE, ASD-STE100, or compliance: read `references/strict-vocabulary.md` before you draft the document, and say once that no tool guarantees compliance. The reply stays Plain in every mode.

When asked to CHECK text instead of writing it, read `references/rule-catalog.md` and report each violation as: rule number, the offending text, a compliant rewrite. Cite only rule numbers from that file, never from memory.

## Limits

These rules are for facts and instructions, not marketing copy or brand writing: they delete persuasion by design. Say so, and offer them for the docs instead.

## References

- `references/rule-catalog.md` — the 53 rules of Issue 9 with software examples, for CHECK mode
- `references/checklist.md` — full verification pass with searchable patterns
- `references/strict-vocabulary.md` — the dictionary discipline for Strict mode
- `references/word-swaps.md` — slop-to-plain word map
- `references/use-cases.md` — patterns for error messages, runbooks, incident reports, release notes, commits, agent prompts, UI copy, i18n
