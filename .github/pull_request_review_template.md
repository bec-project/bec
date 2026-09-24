<!--
Copy this template into a PR review body; GitHub does not load it automatically.
Replace placeholders and remove these instructions and unused optional sections.
Record only checks actually performed. The review applies to the reviewed commit;
check for newer commits before submitting and state any unreviewed changes.

Agents must submit reviews only as COMMENT, regardless of whether findings remain.
Use `gh pr review --comment` or the API review event `COMMENT`. Never submit
APPROVE or REQUEST_CHANGES, or use `--approve` or `--request-changes`.
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
-->

**Review:** COMMENT — {{brief summary of findings or absence of findings}}.

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
