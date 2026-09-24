<!--
Keep the description proportional to the change. Remove sections that do not apply.
Replace the visible prompts below with information about your change.
Write for someone who has not followed the implementation or review discussion.
Update the description when the scope changes so it describes the final change.
These comments are author guidance and will not appear in the rendered description.
-->

## Description

<!--
Explain the problem or motivation, then the resulting behavior and how it is achieved.
For a bug fix, give a concrete trigger and the before/after behavior. Mention related
changes when they affect what reviewers need to understand; avoid a file-by-file recap.

Examples:
- Restarting the device server could leave other services with an outdated device list.
  It now emits a configuration reload event after startup so those services refresh
  their devices. Initial loading and subsequent reloads use the same lock, so a reload
  waits for initialization to finish.
- A typo in a configuration filename could produce a confusing NotImplementedError
  because the loader checked the suffix before checking whether the file existed.
  It now reports the missing path and suggests a likely YAML filename when available.

Include screenshots or before/after output when they help explain the change.
Put lengthy examples or technical details in a <details> block with a <summary>.
-->

[Describe the problem or motivation and the resulting behavior.]

## Related Issues

<!--
Link issues using "Closes #123" when this PR resolves them, or "Related to #123"
when it only contributes. Link companion PRs and documentation changes in bec_docs
where relevant. State any required merge or deployment order.
-->

[Link related issues, companion PRs, and documentation changes.]

## Type of Change

<!--
List the main changes, such as a bug fix, feature, refactor, or documentation update.
Name the affected behavior rather than repeating the description or listing every file.
-->

- [Change and affected behavior]

## How to test

<!--
Give concrete steps and expected outcomes, including any required setup.
Include the exact relevant test command instead of a generic "Run unit tests".
Distinguish instructions for reviewers from checks you already ran. If reporting
completed checks, state their results and any relevant limitations or checks not run.

Examples:
- Start BEC services and an IPython client, then restart only the device server.
  Check that the other services receive the reload event and refresh their device lists.
- With test_config.yaml available, request test_config.yml. Check that the error
  suggests test_config.yaml and that the active configuration remains unchanged.
- Run the affected regression tests:
  python -m pytest --random-order bec_lib/tests/test_device_manager.py
-->

- [Required setup]
- [Test command or manual steps, with expected outcomes]
- [Checks already run and their results, or checks not run]

## Potential side effects

<!--
Describe relevant compatibility changes, affected consumers, changed defaults,
migration steps, or remaining limitations. Be specific about affected data or APIs.
Explain intentional behavior that could otherwise surprise a user or deployer.

Examples:
- Clients using wait_for_server now also wait for DAP. Configuration reload
  notifications remain asynchronous; consumers may finish refreshing at different times.
- Suggested configuration files are never loaded automatically; the user must
  explicitly select the intended configuration.
-->

[Describe compatibility changes, migration needs, or remaining limitations.]

## Screenshots / GIFs (if applicable)

<!--
Include screenshots, GIFs, or before/after output when they help explain the change.
Label the old and new behavior. Remove this section if it does not apply.
-->

[Add relevant screenshots, GIFs, or example output.]

## Additional Comments

<!--
Add context that helps reviewers assess the change, such as a design tradeoff or a
linked follow-up. Remove this section if there is nothing further to add.
-->

[Add any additional context for reviewers.]

## Definition of Done

<!--
Link documentation updates above where relevant. If no documentation change is
needed, replace the checkbox with "Documentation: not applicable" and a brief reason.
-->

- [ ] Documentation is up-to-date.
