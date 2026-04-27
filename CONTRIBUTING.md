# Contributing

Thank you for considering contributing to BEC. Contributions are essential for improving the project and helping it grow.
We welcome bug reports and feature requests via [GitHub issues](https://github.com/bec-project/bec/issues), as well as contributions to the code base and the documentation.

## Reporting Bugs or Requesting Features

- Before submitting a bug report or feature request, please check the [issue tracker](https://github.com/bec-project/bec/issues) to avoid duplication.
- If the issue or feature hasn't been reported, open a new issue with a clear title and description. For bug reports, include reproduction steps, the package versions of the deployed BEC services (for example `bec_lib`), and details about your operating system. This significantly improves the chance that we can reproduce and fix the problem quickly.

## Contributing Code

If you want to contribute code to BEC, please work against the [BEC repository](https://github.com/bec-project/bec).
Before starting, clone the repository locally and set up a developer environment using the installation instructions in the separate documentation repository [`bec-project/bec_docs`](https://github.com/bec-project/bec_docs) or the published installation guide at [bec.readthedocs.io/latest/how-to/general/install-bec-locally.html](https://bec.readthedocs.io/latest/how-to/general/install-bec-locally.html).

Afterwards, you may follow this step-by-step guide to suggest your code improvements:

1. Create a new branch for your changes:

   ```bash
   git checkout -b feature/your-feature
   ```

2. Make your changes.

3. Use Black to format your code:

   ```bash
   black --line-length=100 --skip-magic-trailing-comma .
   ```

4. Use isort to sort your imports:

   ```bash
   isort --line-length=100 --profile=black --multi-line=3 --trailing-comma .
   ```

5. Run Pylint on your code to ensure it meets coding standards:

   ```bash
   pylint your_module_or_package
   ```

6. Write tests for new features or fixed bugs, and add them to the test folder.
   We use [pytest](https://github.com/pytest-dev/pytest) within our team to test code.

7. Follow [Conventional Commit Messages](https://www.conventionalcommits.org/en/v1.0.0/) when writing commit messages. This helps us automatically generate a changelog. For example:

   ```bash
   git commit -m "feat: add new feature"
   ```

   or

   ```bash
   git commit -m "fix: fix bug"
   ```

   or

   ```bash
   git commit -m "docs: update documentation"
   ```

8. Push your commits to the remote branch:

   ```bash
   git push origin feature/your-feature
   ```

9. Open a pull request on GitHub. Include a clear title and description of your changes. If your pull request fixes an issue, include `closes #123` in the description to automatically close the issue when the pull request is merged.

## Contributing Documentation

- Improvements to the documentation are always appreciated.
- BEC documentation and installation instructions are maintained in the separate repository [`bec-project/bec_docs`](https://github.com/bec-project/bec_docs) and are published at [bec.readthedocs.org](https://bec.readthedocs.org/).
- If you find a typo, missing explanation, or outdated installation step, please open an issue or pull request in `bec_docs`.
- For documentation changes, follow the same general workflow as for code contributions: create a branch, make the change in `bec_docs`, and open a pull request there.
