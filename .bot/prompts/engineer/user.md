## Bug to fix — issue #{{issue_number}}

**{{issue_title}}**
{{issue_url}}

### Issue description
{{issue_body}}

---

Reproduce this bug with a failing **live E2E test** (`cargo test --test <suite> --
--ignored`, using `common::live_session()`), then fix the code so it passes, per
the BUG-FIX FLOW and author-system rules. The issue body above is the reporter's
account — verify it against the actual code before deciding what to change; if the
behaviour is already correct, report `no_change_needed` and say where the existing
tests cover it.
