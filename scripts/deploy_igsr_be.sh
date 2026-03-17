#!/usr/bin/env bash
set -euo pipefail

# Deploy the IGSR back-end to test.internationalgenome.org (dev) or internationalgenome.org (prod).
# This script builds the Docker image from the specified path, pushes it to Google Artifact Registry, and deploys it to Cloud Run.
# Use --dry-run first to verify the commands that will be executed without making any changes.
# See example-config.ini for how config.ini should be structured.
#
# Examples:
#   deploy_igsr_be.sh --path ./igsr-be --env dev --config ./config.ini --dry-run
#   deploy_igsr_be.sh --path /full/path/to/igsr-be --env prod --config /full/path/to/config.ini
#
SERVICE="igsr-be"
REPO="igsr"
REGION="europe-west2"
DOCKER_CONTEXT="colima"
BUILDX_BUILDER="multi"

BACKEND_PATH=""
CONFIG_PATH=""
TARGET_ENV=""
BRANCH_NAME="unknown"
MIN_INSTANCES=""
MAX_INSTANCES=""
CORS_ALLOW_ORIGINS=""
PROJECT_ID=""
IMAGE=""
ES_CLOUD_ID=""
ES_API_KEY=""
DEPLOY_ENV_VARS=""
DEPLOY_ENV_VARS_DISPLAY=""
DRY_RUN=0

log() { printf "\n==> %s\n" "$*"; }
die() { printf "\nERROR: %s\n" "$*" >&2; exit 1; }

usage() {
  cat <<EOF
Usage: $(basename "$0") --path PATH --env {dev|prod} --config PATH [--dry-run]

Required:
  --path, -p PATH      Path to igsr-be
  --env, -e ENV        Deployment environment: dev or prod
  --config, -c PATH    Path to config.ini with ES_CLOUD_ID and ES_API_KEY

Optional:
  --dry-run            Print commands without executing
  -h, --help           Show help

Examples:
  $(basename "$0") --path ./igsr-be --env dev --config ./config.ini --dry-run
  $(basename "$0") --path /full/path/to/igsr-be --env prod --config /full/path/to/config.ini
EOF
}

run() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf "DRY RUN: "
    printf "%q " "$@"
    printf "\n"
    return 0
  fi
  "$@"
}

run_in_dir() {
  local dir="$1"
  shift

  if [ "$DRY_RUN" -eq 1 ]; then
    printf "DRY RUN: (cd %q && " "$dir"
    printf "%q " "$@"
    printf ")\n"
    return 0
  fi

  (
    cd "$dir"
    "$@"
  )
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf "%s" "$value"
}

strip_matching_quotes() {
  local value="$1"
  local first_char=""
  local last_char=""

  if [ "${#value}" -ge 2 ]; then
    first_char="${value:0:1}"
    last_char="${value: -1}"
    if { [ "$first_char" = "\"" ] || [ "$first_char" = "'" ]; } && [ "$first_char" = "$last_char" ]; then
      value="${value:1:${#value}-2}"
    fi
  fi

  printf "%s" "$value"
}

to_lower() {
  printf "%s" "$1" | tr '[:upper:]' '[:lower:]'
}

read_ini_value() {
  local key="$1"
  local value=""

  value="$(
    awk -F= -v key="$key" '
      /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
      $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
        line = $0
        sub(/^[[:space:]]*[^=]+=[[:space:]]*/, "", line)
        print line
        exit
      }
    ' "$CONFIG_PATH"
  )"

  value="$(trim "$value")"
  value="$(strip_matching_quotes "$value")"
  printf "%s" "$value"
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --path|-p)
        [ "$#" -ge 2 ] || die "Missing value for $1"
        BACKEND_PATH="$2"
        shift 2
        ;;
      --env|-e)
        [ "$#" -ge 2 ] || die "Missing value for $1"
        TARGET_ENV="$2"
        shift 2
        ;;
      --config|-c)
        [ "$#" -ge 2 ] || die "Missing value for $1"
        CONFIG_PATH="$2"
        shift 2
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "Unknown argument: $1"
        ;;
    esac
  done
}

validate_inputs() {
  [ -n "$BACKEND_PATH" ] || die "--path is required"
  [ -n "$TARGET_ENV" ] || die "--env is required"
  [ -n "$CONFIG_PATH" ] || die "--config is required"
  [ -d "$BACKEND_PATH" ] || die "Path does not exist: $BACKEND_PATH"
  [ -f "$BACKEND_PATH/Dockerfile" ] || die "No Dockerfile found at: $BACKEND_PATH/Dockerfile"
  [ -f "$CONFIG_PATH" ] || die "Config file does not exist: $CONFIG_PATH"

  BACKEND_PATH="$(cd "$BACKEND_PATH" && pwd -P)"
  CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd -P)/$(basename "$CONFIG_PATH")"

  case "$TARGET_ENV" in
    dev|prod) ;;
    *)
      die "--env must be 'dev' or 'prod' (got: $TARGET_ENV)"
      ;;
  esac
}

load_config() {
  local es_cloud_id_lc=""

  ES_CLOUD_ID="$(read_ini_value "ES_CLOUD_ID")"
  ES_API_KEY="$(read_ini_value "ES_API_KEY")"

  [ -n "$ES_CLOUD_ID" ] || die "ES_CLOUD_ID is missing or empty in: $CONFIG_PATH"
  [ -n "$ES_API_KEY" ] || die "ES_API_KEY is missing or empty in: $CONFIG_PATH"

  es_cloud_id_lc="$(to_lower "$ES_CLOUD_ID")"

  case "$es_cloud_id_lc" in
    *"$TARGET_ENV"*) ;;
    *)
      die "ES_CLOUD_ID must contain '$TARGET_ENV' for --env $TARGET_ENV"
      ;;
  esac
}

set_env_values() {
  case "$TARGET_ENV" in
    dev)
      PROJECT_ID="prj-ext-dev-gaa-igsr"
      IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/igsr-be:dev"
      CORS_ALLOW_ORIGINS='["https://test.internationalgenome.org"]'
      MIN_INSTANCES="0"
      MAX_INSTANCES="1"
      ;;
    prod)
      PROJECT_ID="prj-ext-prod-gaa-igsr"
      IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/igsr-be:prod"
      CORS_ALLOW_ORIGINS='["https://internationalgenome.org"]'
      MIN_INSTANCES="1"
      MAX_INSTANCES="2"
      ;;
  esac

  DEPLOY_ENV_VARS="ES_CLOUD_ID=$ES_CLOUD_ID,ES_API_KEY=$ES_API_KEY,CORS_ALLOW_ORIGINS=$CORS_ALLOW_ORIGINS"
  DEPLOY_ENV_VARS_DISPLAY="ES_CLOUD_ID=$ES_CLOUD_ID,ES_API_KEY=<redacted>,CORS_ALLOW_ORIGINS=$CORS_ALLOW_ORIGINS"
}

detect_branch() {
  if ! command -v git >/dev/null 2>&1; then
    BRANCH_NAME="git-not-installed"
    return 0
  fi

  if ! git -C "$BACKEND_PATH" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    BRANCH_NAME="not-a-git-repo"
    return 0
  fi

  BRANCH_NAME="$(git -C "$BACKEND_PATH" branch --show-current 2>/dev/null || true)"
  if [ -z "$BRANCH_NAME" ]; then
    BRANCH_NAME="detached-head"
  fi
}

run_deploy() {
  local deploy_args=(
    run deploy "$SERVICE"
    --image "$IMAGE"
    --allow-unauthenticated
    --region "$REGION"
    --concurrency 80
    --memory 512Mi
    --min-instances "$MIN_INSTANCES"
    --max-instances "$MAX_INSTANCES"
    --set-env-vars "$DEPLOY_ENV_VARS"
  )

  if [ "$DRY_RUN" -eq 1 ]; then
    local dry_run_args=(
      run deploy "$SERVICE"
      --image "$IMAGE"
      --allow-unauthenticated
      --region "$REGION"
      --concurrency 80
      --memory 512Mi
      --min-instances "$MIN_INSTANCES"
      --max-instances "$MAX_INSTANCES"
      --set-env-vars "$DEPLOY_ENV_VARS_DISPLAY"
    )
    printf "DRY RUN: "
    printf "%q " gcloud "${dry_run_args[@]}"
    printf "\n"
    return 0
  fi

  gcloud "${deploy_args[@]}"
}

main() {
  parse_args "$@"
  validate_inputs
  load_config
  set_env_values
  detect_branch

  if [ "$DRY_RUN" -eq 0 ]; then
    need_cmd docker
    need_cmd colima
    need_cmd gcloud
    need_cmd awk
  fi

  log "Deployment plan"
  printf "BACKEND_PATH=%s\n" "$BACKEND_PATH"
  printf "CONFIG_PATH=%s\n" "$CONFIG_PATH"
  printf "TARGET_ENV=%s\n" "$TARGET_ENV"
  printf "BRANCH_NAME=%s\n" "$BRANCH_NAME"
  printf "IMAGE=%s\n" "$IMAGE"
  printf "SERVICE=%s\n" "$SERVICE"
  printf "PROJECT_ID=%s\n" "$PROJECT_ID"
  printf "CORS_ALLOW_ORIGINS=%s\n" "$CORS_ALLOW_ORIGINS"
  printf "MIN_INSTANCES=%s\n" "$MIN_INSTANCES"
  printf "MAX_INSTANCES=%s\n" "$MAX_INSTANCES"
  printf "ES_CLOUD_ID_CHECK=contains '%s'\n" "$TARGET_ENV"
  printf "ES_API_KEY=%s\n" "<redacted>"

  run docker context use "$DOCKER_CONTEXT"
  run colima start

  run gcloud config set project "$PROJECT_ID"
  run gcloud config set run/region "$REGION"
  run gcloud services enable \
    run.googleapis.com \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com
  run gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet

  run docker buildx use "$BUILDX_BUILDER"
  run_in_dir "$BACKEND_PATH" \
    docker buildx build \
      --platform linux/amd64 \
      -t "$IMAGE" \
      --no-cache \
      --push \
      .

  run_deploy

  log "Done"
}

main "$@"
