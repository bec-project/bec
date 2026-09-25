<!--
Copy this template into a top-level PR conversation comment; GitHub does not load it automatically.
Replace placeholders and remove these instructions and unused optional sections.
Record only checks actually performed. The review applies to the reviewed commit;
check for newer commits before submitting and state any unreviewed changes.

The verdict expresses the reviewer's assessment:
- APPROVE: no required fixes remain. Optional improvements do not prevent approval.
- REQUEST CHANGES: required fixes remain; describe them in the findings.

Agents must publish the assessment as a top-level PR conversation comment using
`gh pr comment --body-file <path>` or the issue-comments API. The freshness workflow
updates these comments; it does not manage review submissions or inline comments.
Do not use `gh pr review` or the GitHub Reviews API to publish this summary, and
never submit a formal `APPROVE` or `REQUEST_CHANGES` event.
Do not use COMMENT as the verdict: it describes how the review is posted,
not whether the change is acceptable.

Summarize required fixes or the absence of findings in the visible review summary.
Include the full model designation supplied by the review environment: model
family/version, variant, and reasoning effort, for example "GPT-6 Astra Ultra".
Do not shorten it to just "GPT-6" or omit known settings. Do not guess missing
details; mark unavailable components as "not reported". For a human review,
omit the model field.
Before posting, check for an existing review on the same PR with the exact same
full model designation. Replace that review rather than adding a duplicate,
updating its summary, reviewed commit, findings, and validation. Keep reviews
from different full model designations separate.
Use immutable commit and source links. If local changes were also reviewed,
describe them explicitly: the commit alone does not identify that review scope.

Keep the `bec-review` HTML marker below and the exact `**Reviewed commit:**`
field, with the full 40-character PR head SHA as both the link text and the commit
URL suffix. Do not replace it with a branch name or the PR's test merge commit.
The Review comment freshness workflow compares this SHA with the live PR head on
pushes and comment creation/edits. It adds a STALE REVIEW warning when they differ
and removes it when they match again, without changing the verdict or review text.
Only update the referenced SHA after reviewing that revision. Leave any generated
`bec-review-stale:start` / `bec-review-stale:end` warning block to the workflow.
The workflow also recognizes older comments with the Reviewer and Reviewed commit
fields. It becomes active after reaching the default branch; workflow_dispatch
can check an existing PR without waiting for another push or comment edit.
-->

<!-- bec-review -->
**Verdict:** {{APPROVE / REQUEST CHANGES}} — {{brief summary of findings or absence of findings}}.

**Reviewer:** {{reviewer or agent name}}
**Model:** {{full model name, variant, and reasoning effort, e.g. GPT-6 Astra Ultra}}
**Reviewed commit:** [{{full head SHA}}]({{repository URL}}/commit/{{full head SHA}})

<details>
<summary>Review details</summary>

**Base:** [{{full base SHA}}]({{repository URL}}/commit/{{full base SHA}})
**Scope:** {{head branch}} against {{base branch}}; {{files and behaviors reviewed}}.

### Findings introduced or exposed by this change

<!--
If none, replace the finding block with:
"No remaining introduced or exposed findings were identified."
Otherwise, repeat the block below, most severe first, up to ten findings.
"Introduced" means caused by the change; "exposed" means an existing defect is
made reachable, more likely, or worse by the change. Keep inherited issues below.
CONFIRMED requires a reproduction or concrete code path; PLAUSIBLE requires a
specific realistic failure scenario. Distinguish required fixes from suggestions.
-->

#### 1. [{{introduced / exposed}}] {{one-line defect}}

**Severity:** {{severity and whether a fix is required}} · **Confidence:** {{CONFIRMED / PLAUSIBLE}}
**Location:** [{{file}}:{{line}}]({{immutable source permalink}})

{{Concrete trigger, incorrect behavior, impact, and how this change causes or exposes it.}}

**Evidence:** {{test result, reproduction, or concrete code path}}.
**Suggested fix:** {{specific change; name an existing helper or hook where appropriate}}.

### What was checked and found sound

- {{Author claim or risky behavior checked, and the evidence supporting it.}}
- {{Material assumption or explicitly accepted behavior, if relevant.}}

### Validation

- `{{exact command}}` → {{result, including passed/failed/skipped counts where relevant}}.
- {{Relevant environment, simulated services, or manual reproduction and result.}}
- **Limitations:** {{checks not run, unverified claims, or gaps and their effect on the assessment; omit if none}}.

### Optional improvements

<!-- Omit if none. Required fixes belong in findings, not here. -->
- {{Non-blocking suggestion within the change; cite the rule for convention feedback.}}

### Pre-existing issues — separate, non-blocking follow-ups

<!-- Omit if none. Explain why each issue predates and is not worsened by the change. -->
- [{{file}}:{{line}}]({{immutable source permalink}}) — {{issue, impact, and evidence it is pre-existing}}.

</details>
