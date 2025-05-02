# bilanc-tap-bitbucket

A [Singer](https://singer.io) tap for extracting data from Bitbucket.

## Configuration

This tap requires a `config.json` file that specifies:

```json
{
  "access_token": "your_access_token",
  "repository": "workspace/repo-name",
  "start_date": "2021-01-01T00:00:00Z",
  "request_timeout": 300
}
```

- `access_token`: A valid Bitbucket access token
- `repository`: The repository to extract data from in the format `workspace/repository-name`
- `start_date`: The date from which to start extracting data
- `request_timeout`: The request timeout in seconds (default: 300)

You can also provide OAuth credentials if you have them:

```json
{
  "client_id": "your_client_id",
  "client_secret": "your_client_secret",
  "refresh_token": "your_refresh_token",
  "start_date": "2021-01-01T00:00:00Z",
  "repository": "workspace/repo-name",
  "request_timeout": 300
}
```

## Usage

```bash
# Install
pip install -e .

# Discovery mode
tap-bitbucket --config config.json --discover > catalog.json

# Sync mode
tap-bitbucket --config config.json --catalog catalog.json
```
