import functools
import os
from typing import Literal

import requests
from github import Github
from pydantic import BaseModel


class GHConfig(BaseModel):
    token: str
    organization: str
    repository: str
    project_number: int
    graphql_url: str
    rest_url: str
    headers: dict


class ProjectItemHandler:
    """
    A class to handle GitHub project items.
    """

    def __init__(self, gh_config: GHConfig):
        self.gh_config = gh_config
        self.gh = Github(gh_config.token)
        self.repo = self.gh.get_repo(f"{gh_config.organization}/{gh_config.repository}")
        self.project_node_id = self.get_project_node_id()

    def set_issue_status(
        self,
        status: Literal[
            "Selected for Development",
            "Weekly Backlog",
            "In Development",
            "Ready For Review",
            "On Hold",
            "Done",
        ],
        issue_number: int | None = None,
        issue_node_id: str | None = None,
    ):
        """
        Set the status field of a GitHub issue in the project.

        Args:
            status (str): The status to set. Must be one of the predefined statuses.
            issue_number (int, optional): The issue number. If not provided, issue_node_id must be provided.
            issue_node_id (str, optional): The issue node ID. If not provided, issue_number must be provided.
        """
        if not issue_number and not issue_node_id:
            raise ValueError("Either issue_number or issue_node_id must be provided.")
        if issue_number and issue_node_id:
            raise ValueError("Only one of issue_number or issue_node_id must be provided.")
        if issue_number is not None:
            issue = self.repo.get_issue(issue_number)
            issue_id = self.get_issue_info(issue.node_id)[0]["id"]
        else:
            issue_id = issue_node_id
        field_id, option_id = self.get_status_field_id(field_name=status)
        self.set_field_option(issue_id, field_id, option_id)

    def run_graphql(self, query: str, variables: dict) -> dict:
        """
        Execute a GraphQL query against the GitHub API.

        Args:
            query (str): The GraphQL query to execute.
            variables (dict): The variables to pass to the query.

        Returns:
            dict: The response from the GitHub API.
        """
        response = requests.post(
            self.gh_config.graphql_url,
            json={"query": query, "variables": variables},
            headers=self.gh_config.headers,
            timeout=10,
        )
        if response.status_code != 200:
            raise Exception(
                f"Query failed with status code {response.status_code}: {response.text}"
            )
        return response.json()

    def get_project_node_id(self):
        """
        Retrieve the project node ID from the GitHub API.
        """
        query = """
      query($owner: String!, $number: Int!) {
        organization(login: $owner) {
          projectV2(number: $number) {
            id
          }
        }
      }
      """
        variables = {"owner": self.gh_config.organization, "number": self.gh_config.project_number}
        resp = self.run_graphql(query, variables)
        return resp["data"]["organization"]["projectV2"]["id"]

    def get_issue_info(self, issue_node_id: str):
        """
        Get the project-related information for a given issue node ID.

        Args:
            issue_node_id (str): The node ID of the issue. Please note that this is not the issue number and typically starts with "I".

        Returns:
            list[dict]: A list of project items associated with the issue.
        """
        query = """
        query($issueId: ID!) {
          node(id: $issueId) {
            ... on Issue {
              projectItems(first: 10) {
                nodes {
                  project {
                    id
                    title
                  }
                  id
                  fieldValues(first: 20) {
                    nodes {
                      ... on ProjectV2ItemFieldSingleSelectValue {
                        name
                        field {
                          ... on ProjectV2SingleSelectField {
                            name
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        variables = {"issueId": issue_node_id}
        resp = self.run_graphql(query, variables)
        return resp["data"]["node"]["projectItems"]["nodes"]

    def get_status_field_id(
        self,
        field_name: Literal[
            "Selected for Development",
            "Weekly Backlog",
            "In Development",
            "Ready For Review",
            "On Hold",
            "Done",
        ],
    ) -> tuple[str, str]:
        """
        Get the status field ID and option ID for the given field name in the project.

        Args:
            field_name (str): The name of the field to retrieve.
                Must be one of the predefined statuses.

        Returns:
            tuple[str, str]: A tuple containing the field ID and option ID.
        """
        field_id = None
        option_id = None
        project_fields = self.get_project_fields()
        for field in project_fields:
            if field["name"] != "Status":
                continue
            field_id = field["id"]
            for option in field["options"]:
                if option["name"] == field_name:
                    option_id = option["id"]
                    break
        if not field_id or not option_id:
            raise ValueError(f"Field '{field_name}' not found in project fields.")

        return field_id, option_id

    def set_field_option(self, item_id, field_id, option_id):
        """
        Set the option of a project item for a single-select field.

        Args:
            item_id (str): The ID of the project item to update.
            field_id (str): The ID of the field to update.
            option_id (str): The ID of the option to set.
        """

        mutation = """
        mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
          updateProjectV2ItemFieldValue(
            input: {
              projectId: $projectId
              itemId: $itemId
              fieldId: $fieldId
              value: { singleSelectOptionId: $optionId }
            }
          ) {
            projectV2Item {
              id
            }
          }
        }
        """
        variables = {
            "projectId": self.project_node_id,
            "itemId": item_id,
            "fieldId": field_id,
            "optionId": option_id,
        }
        return self.run_graphql(mutation, variables)

    @functools.lru_cache(maxsize=1)
    def get_project_fields(self) -> list[dict]:
        """
        Get the available fields in the project.
        This method caches the result to avoid multiple API calls.

        Returns:
            list[dict]: A list of fields in the project.
        """

        query = """
    query($projectId: ID!) {
      node(id: $projectId) {
        ... on ProjectV2 {
          fields(first: 50) {
            nodes {
              ... on ProjectV2SingleSelectField {
                id
                name
                options {
                  id
                  name
                }
              }
            }
          }
        }
      }
    }
    """
        variables = {"projectId": self.project_node_id}
        resp = self.run_graphql(query, variables)
        return list(filter(bool, resp["data"]["node"]["fields"]["nodes"]))

    def get_pull_request_linked_issues(self, pr_number: int) -> list[dict]:
        """
        Get the linked issues of a pull request.

        Args:
            pr_number (int): The pull request number.

        Returns:
            list[dict]: A list of linked issues.
        """
        query = """
    query($number: Int!, $owner: String!, $repo: String!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $number) {
          id
          closingIssuesReferences(first: 50) {
            edges {
              node {
                id
                body
                number
                title
              }
            }
          }
        }
      }
    }
    """
        variables = {
            "number": pr_number,
            "owner": self.gh_config.organization,
            "repo": self.gh_config.repository,
        }
        resp = self.run_graphql(query, variables)
        edges = resp["data"]["repository"]["pullRequest"]["closingIssuesReferences"]["edges"]
        return [edge["node"] for edge in edges if edge.get("node")]


def main():
    # GitHub settings
    token = os.getenv("TOKEN")
    org = os.getenv("ORG")
    repo = os.getenv("REPO")
    project_number = os.getenv("PROJECT_NUMBER")
    pr_number = os.getenv("PR_NUMBER")

    if not token:
        raise ValueError("GitHub token is not set. Please set the TOKEN environment variable.")
    if not org:
        raise ValueError("GitHub organization is not set. Please set the ORG environment variable.")
    if not repo:
        raise ValueError("GitHub repository is not set. Please set the REPO environment variable.")
    if not project_number:
        raise ValueError(
            "GitHub project number is not set. Please set the PROJECT_NUMBER environment variable."
        )
    if not pr_number:
        raise ValueError(
            "Pull request number is not set. Please set the PR_NUMBER environment variable."
        )

    project_number = int(project_number)
    pr_number = int(pr_number)

    gh_config = GHConfig(
        token=token,
        organization=org,
        repository=repo,
        project_number=project_number,
        graphql_url="https://api.github.com/graphql",
        rest_url=f"https://api.github.com/repos/{org}/{repo}/issues",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
    )
    project_item_handler = ProjectItemHandler(gh_config=gh_config)

    # Get PR info
    pr = project_item_handler.repo.get_pull(pr_number)

    # Get the linked issues of the pull request
    linked_issues = project_item_handler.get_pull_request_linked_issues(pr_number=pr_number)
    print(f"Linked issues: {linked_issues}")

    target_status = "In Development" if pr.draft else "Ready For Review"
    print(f"Target status: {target_status}")
    for issue in linked_issues:
        project_item_handler.set_issue_status(issue_number=issue["number"], status=target_status)


if __name__ == "__main__":
    main()
