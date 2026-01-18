#!/usr/bin/env bash
#
# Pi-PreClear NG (Unified Pipeline)
# - Single pipeline (no legacy/unraid pipeline)
# - Full-disk stress: pre-read -> badblocks (destructive patterns) -> zero-fill -> post-read
# - Thermal monitoring with pause/resume/abort
# - SMART deltas shown in a classic (Unraid-like) UI layout with SMART box pinned at bottom
# - Resume at step boundaries (safe key=value state file; no sourcing arbitrary code)
#
# WARNING: This is destructive to the target disk.

set -Eeuo pipefail

LC_CTYPE=C
export LC_CTYPE

# -----------------------------
# Version / constants
# -----------------------------
VERSION="1.1.0"
PLATFORM_NAME="Pi"

readonly UI_WIDTH=120
readonly UI_HEIGHT_TOP=11
readonly UI_HEIGHT_SMART=14
readonly DEFAULT_REFRESH_S=5
readonly DEFAULT_SMART_REFRESH_S=300
readonly DEFAULT_TEMP_POLL_S=5
readonly DEFAULT_FAIL_MIN=10

readonly DD_BS_READ="4M"
readonly DD_BS_WRITE="4M"

# Hang detection: seconds with no byte progress before escalation
readonly DD_HANG_WARN_S=600      # 10 minutes
readonly DD_HANG_KILL_S=1200     # 20 minutes

# State / reports
PC_BASE="/var/lib/preclear-ng"
PC_PLUGIN_DIR="${PC_BASE}/pi-preclear"
PC_REPORT_DIR="${PC_BASE}/preclear_reports"
PC_TMP_DIR="/tmp/.preclear"

# Fallback for non-root environments (still requires root to run destructive steps)
if [[ ! -d "$PC_BASE" ]]; then
  PC_BASE="$HOME/.preclear-ng"
  PC_PLUGIN_DIR="${PC_BASE}/pi-preclear"
  PC_REPORT_DIR="${PC_BASE}/preclear_reports"
  PC_TMP_DIR="${PC_BASE}/tmp"
fi

mkdir -p "$PC_PLUGIN_DIR" "$PC_REPORT_DIR" "$PC_TMP_DIR" 2>/dev/null || true

# Logging
LOG_FILE="/var/log/preclear.disk.log"
if [[ ! -d /var/log || ! -w /var/log ]]; then
  LOG_FILE="${PC_BASE}/preclear.disk.log"
fi

# -----------------------------
# Globals (kept minimal)
# -----------------------------
DISK=""
CYCLES=1
NO_PROMPT="n"
RESUME="n"

# Steps: 1..6 (6 = certificate/report)
START_STEP=1

# Pipeline toggles
SKIP_PREREAD="n"
SKIP_BADBLOCKS="n"
SKIP_ZERO="n"
SKIP_POSTREAD="n"

# badblocks options
BB_BLOCKSIZE=""              # if empty, auto from logical sector size
BB_PATTERNS="0xaa,0x55,0xff,0x00"

# SMART options
SMART_TYPE="auto"            # auto|sat|scsi|ata|nvme
SMART_LONG="n"               # run a long test before final SMART snapshot

# UI / refresh
REFRESH_S=$DEFAULT_REFRESH_S
SMART_REFRESH_S=$DEFAULT_SMART_REFRESH_S

# Thermal controls (defaults set after we detect HDD/SSD)
TEMP_ENABLE="y"
TEMP_POLL_S=$DEFAULT_TEMP_POLL_S
TEMP_PAUSE_C=50
TEMP_RESUME_C=45
TEMP_ABORT_C=55
TEMP_FAIL_MIN=$DEFAULT_FAIL_MIN

# Derived / runtime
DISK_SERIAL=""
DISK_MODEL=""
DISK_SIZE_BYTES=0
DISK_SECTOR_BYTES=512
DISK_ROTA=1

STATE_FILE=""
CERT_FILE=""

# Runtime stats
STEP_NUM=1
STEP_NAME=""
STEP_STARTED_AT=0
TOTAL_STARTED_AT=0

CUR_BYTES=0
CUR_SPEED=""
AVG_SPEED=""
PERCENT=0

# Thermal stats
TEMP_CUR=""
TEMP_MIN=""
TEMP_MAX=""
TEMP_STEP_MIN=""
TEMP_STEP_MAX=""
TEMP_PAUSED_SECONDS=0
TEMP_ABOVE_PAUSE_SECONDS=0
TEMP_LAST_POLL=0
TEMP_IS_PAUSED="n"

# SMART snapshots
SMART_INITIAL_FILE=""
SMART_LAST_FILE=""
SMART_LAST_AT=0

# Child process tracking
CHILD_PID=0
CHILD_KIND=""  # dd|badblocks
CHILD_PAUSED="n"
CHILD_LAST_BYTES=0
CHILD_LAST_PROGRESS_AT=0

# -----------------------------
# Helpers
# -----------------------------
log() {
  local msg="$*"
  local ts
  ts="$(date '+%b %d %T')"
  printf '%s preclear-ng: %s\n' "$ts" "$msg" >> "$LOG_FILE" 2>/dev/null || true
}

die() {
  log "FATAL: $*"
  echo "ERROR: $*" >&2
  exit 1
}

is_root() { [[ "${EUID:-$(id -u)}" -eq 0 ]]; }

is_number() { [[ "$1" =~ ^[0-9]+$ ]]; }

human_bytes() {
  local b="$1"
  awk -v b="$b" 'BEGIN{
    split("B KB MB GB TB PB", u, " ");
    i=1;
    while (b>=1024 && i<6){b/=1024; i++}
    if (i==1) printf "%.0f %s", b, u[i];
    else printf "%.1f %s", b, u[i];
  }'
}

hr_time() {
  local s="$1"
  awk -v s="$s" 'BEGIN{
    h=int(s/3600); m=int((s%3600)/60); ss=int(s%60);
    printf "%d:%02d:%02d", h,m,ss;
  }'
}

box_line() {
  local char="#"; local n=$UI_WIDTH
  printf '%*s\n' "$n" '' | tr ' ' "$char"
}

pad_center() {
  local text="$1"; local width=$UI_WIDTH
  local len=${#text}
  if (( len >= width-2 )); then
    printf '# %s #\n' "${text:0:width-4}"
    return
  fi
  local pad=$(( (width-4-len)/2 ))
  local left=$(printf '%*s' "$pad" '')
  local right=$(printf '%*s' "$((width-4-len-pad))" '')
  printf '# %s%s%s #\n' "$left" "$text" "$right"
}

pad_lr() {
  local left="$1"; local right="$2"; local width=$UI_WIDTH
  local l=${#left}; local r=${#right}
  local space=$((width-4-l-r))
  (( space < 1 )) && space=1
  printf '# %s%*s%s #\n' "$left" "$space" '' "$right"
}

clear_screen() {
  if [[ -t 1 ]]; then
    tput clear || true
  fi
}

# -----------------------------
# Disk / SMART detection
# -----------------------------
smartctl_type_arg() {
  case "$SMART_TYPE" in
    auto) echo "";;
    sat)  echo "-d sat";;
    scsi) echo "-d scsi";;
    ata)  echo "-d ata";;
    nvme) echo "-d nvme";;
    *)    echo "";;
  esac
}

smartctl_run() {
  # Streams to stdout; caller can redirect. Returns smartctl exit.
  local disk="$1"
  local type_args
  type_args="$(smartctl_type_arg)"
  # shellcheck disable=SC2086
  timeout -s 9 30 smartctl --all ${type_args} "$disk" 2>/dev/null
}

smart_has_info() {
  local disk="$1"
  if smartctl_run "$disk" | grep -q "START OF INFORMATION SECTION"; then
    return 0
  fi
  return 1
}

smart_get_temp() {
  # best-effort: return numeric C or empty
  local f="$1"
  local t=""
  # Prefer Temperature_Celsius, then Airflow_Temperature_Cel
  t=$(grep -E "^(190|194)[[:space:]]+" "$f" 2>/dev/null | awk 'NR==1{print $(NF-1)}' | head -n1 || true)
  if [[ -z "$t" ]]; then
    # Some outputs have "Temperature:" in info section
    t=$(grep -E "Temperature:" "$f" 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9]+$/){print $i; exit}}' || true)
  fi
  if is_number "${t:-}"; then
    echo "$t"
  else
    echo ""
  fi
}

smart_extract_attr() {
  # Args: file, attr_name -> value
  local f="$1"; local name="$2"
  awk -v n="$name" '
    $0 ~ /^[0-9]+[[:space:]]+/ {
      # smartctl -A: NAME in column 2
      if ($2==n){print $(NF-1); exit}
    }
  ' "$f" 2>/dev/null || true
}

smart_snapshot() {
  local out="$1"
  if smart_has_info "$DISK"; then
    smartctl_run "$DISK" > "$out" || true
  else
    : > "$out"
  fi
}

detect_disk_identity() {
  DISK_SIZE_BYTES=$(blockdev --getsize64 "$DISK" 2>/dev/null || echo 0)
  DISK_SECTOR_BYTES=$(blockdev --getss "$DISK" 2>/dev/null || echo 512)
  DISK_ROTA=$(lsblk -dn -o ROTA "$DISK" 2>/dev/null | head -n1 || echo 1)

  # udev for model/serial
  local u
  u=$(udevadm info --query=property --name="$DISK" 2>/dev/null || true)
  DISK_MODEL=$(printf '%s\n' "$u" | awk -F= '/^ID_MODEL=/{print $2; exit}')
  DISK_SERIAL=$(printf '%s\n' "$u" | awk -F= '/^ID_SERIAL_SHORT=/{print $2; exit}')

  # fallbacks
  [[ -z "$DISK_MODEL" ]] && DISK_MODEL=$(lsblk -dn -o MODEL "$DISK" 2>/dev/null | head -n1 | xargs || true)
  [[ -z "$DISK_SERIAL" ]] && DISK_SERIAL=$(lsblk -dn -o SERIAL "$DISK" 2>/dev/null | head -n1 | xargs || true)
  [[ -z "$DISK_SERIAL" ]] && DISK_SERIAL="$(basename "$DISK")"

  # defaults per drive type
  if [[ "$DISK_ROTA" == "0" ]]; then
    # SSD / flash
    TEMP_PAUSE_C=${TEMP_PAUSE_C:-60}
    TEMP_RESUME_C=${TEMP_RESUME_C:-55}
    TEMP_ABORT_C=${TEMP_ABORT_C:-70}
  else
    TEMP_PAUSE_C=${TEMP_PAUSE_C:-50}
    TEMP_RESUME_C=${TEMP_RESUME_C:-45}
    TEMP_ABORT_C=${TEMP_ABORT_C:-55}
  fi
}

# -----------------------------
# Safety checks
# -----------------------------
is_preclear_candidate() {
  local disk="$1"
  [[ -b "$disk" ]] || return 1

  # Refuse if disk OR any partitions mounted
  if lsblk -nro MOUNTPOINTS "$disk" 2>/dev/null | grep -qE '\S'; then
    return 1
  fi

  # Refuse if root is on this disk
  local root_src
  root_src=$(findmnt -nro SOURCE / 2>/dev/null || true)
  if [[ -n "$root_src" ]]; then
    local root_disk
    root_disk=$(lsblk -no PKNAME "$root_src" 2>/dev/null | head -n1 || true)
    if [[ -n "$root_disk" ]] && [[ "$disk" == "/dev/$root_disk" ]]; then
      return 1
    fi
  fi

  return 0
}

list_candidates() {
  echo "========================================"
  echo " Disks not currently mounted"
  echo " (potential candidates for preclear)"
  echo "========================================"
  local name disk model serial size
  while read -r name; do
    [[ "$name" =~ ^(loop|zram|sr|mmcblk) ]] && continue
    disk="/dev/$name"
    [[ -b "$disk" ]] || continue
    if ! is_preclear_candidate "$disk"; then
      continue
    fi
    model=$(lsblk -dn -o MODEL "$disk" 2>/dev/null | head -n1 | xargs || true)
    serial=$(lsblk -dn -o SERIAL "$disk" 2>/dev/null | head -n1 | xargs || true)
    size=$(lsblk -dn -o SIZE "$disk" 2>/dev/null | head -n1 | xargs || true)
    printf '%-10s %-10s %-30s %-20s\n' "$disk" "$size" "${model:-?}" "${serial:-?}"
  done < <(lsblk -dn -o NAME)
}

confirm_destruction() {
  if [[ "$NO_PROMPT" == "y" ]]; then
    return 0
  fi
  echo ""
  echo "WARNING: This will DESTRUCTIVELY test and overwrite: $DISK"
  echo "Model: ${DISK_MODEL:-?}  Serial: ${DISK_SERIAL:-?}  Size: $(human_bytes "$DISK_SIZE_BYTES")"
  echo ""
  read -r -p "Type YES to continue: " ans
  [[ "$ans" == "YES" ]] || die "User aborted."
}

require_deps() {
  command -v dd >/dev/null 2>&1 || die "Missing dd"
  command -v badblocks >/dev/null 2>&1 || die "Missing badblocks (e2fsprogs)"
  command -v smartctl >/dev/null 2>&1 || die "Missing smartctl (smartmontools)"
  command -v timeout >/dev/null 2>&1 || die "Missing timeout"
}

# -----------------------------
# State file (safe parsing)
# -----------------------------
write_state() {
  local step="$1"
  local cycle="$2"
  local f="$STATE_FILE"
  umask 077
  {
    echo "step_num=$step"
    echo "cycle_num=$cycle"
    echo "temp_min=${TEMP_MIN:-}"
    echo "temp_max=${TEMP_MAX:-}"
    echo "temp_paused_s=${TEMP_PAUSED_SECONDS:-0}"
    echo "temp_above_pause_s=${TEMP_ABOVE_PAUSE_SECONDS:-0}"
  } > "$f" 2>/dev/null || true
}

read_state() {
  local f="$STATE_FILE"
  [[ -f "$f" ]] || return 1

  # safe ownership & perms: owned by root, not group/world-writable
  local st
  st=$(stat -c '%u %a' "$f" 2>/dev/null || echo "")
  if [[ -n "$st" ]]; then
    local uid perm
    uid=${st%% *}; perm=${st##* }
    if [[ "$uid" != "0" ]]; then
      log "State file not owned by root; ignoring: $f"
      return 1
    fi
    # perm is octal like 600; reject if group/world write
    if (( (10#$perm) & 22 )); then
      log "State file group/world-writable; ignoring: $f"
      return 1
    fi
  fi

  local k v
  while IFS='=' read -r k v; do
    case "$k" in
      step_num) if is_number "$v" && (( v>=1 && v<=6 )); then START_STEP="$v"; fi;;
      cycle_num) if is_number "$v" && (( v>=1 )); then :; fi;;
      temp_min) if is_number "$v"; then TEMP_MIN="$v"; fi;;
      temp_max) if is_number "$v"; then TEMP_MAX="$v"; fi;;
      temp_paused_s) if is_number "$v"; then TEMP_PAUSED_SECONDS="$v"; fi;;
      temp_above_pause_s) if is_number "$v"; then TEMP_ABOVE_PAUSE_SECONDS="$v"; fi;;
    esac
  done < "$f"
  return 0
}

# -----------------------------
# Thermal monitoring
# -----------------------------
thermal_init_for_step() {
  TEMP_STEP_MIN=""
  TEMP_STEP_MAX=""
  TEMP_IS_PAUSED="n"
  CHILD_PAUSED="n"
  TEMP_LAST_POLL=$(date +%s)
}

thermal_update_minmax() {
  local t="$1"
  if ! is_number "$t"; then
    return 0
  fi
  # global min/max
  if [[ -z "${TEMP_MIN:-}" || "$t" -lt "$TEMP_MIN" ]]; then TEMP_MIN="$t"; fi
  if [[ -z "${TEMP_MAX:-}" || "$t" -gt "$TEMP_MAX" ]]; then TEMP_MAX="$t"; fi
  # step min/max
  if [[ -z "${TEMP_STEP_MIN:-}" || "$t" -lt "$TEMP_STEP_MIN" ]]; then TEMP_STEP_MIN="$t"; fi
  if [[ -z "${TEMP_STEP_MAX:-}" || "$t" -gt "$TEMP_STEP_MAX" ]]; then TEMP_STEP_MAX="$t"; fi
}

child_pause() {
  local pid="$1"
  if [[ "$pid" -gt 0 ]] && kill -0 "$pid" 2>/dev/null; then
    kill -STOP "$pid" 2>/dev/null || true
    CHILD_PAUSED="y"
    TEMP_IS_PAUSED="y"
  fi
}

child_resume() {
  local pid="$1"
  if [[ "$pid" -gt 0 ]] && kill -0 "$pid" 2>/dev/null; then
    kill -CONT "$pid" 2>/dev/null || true
    CHILD_PAUSED="n"
    TEMP_IS_PAUSED="n"
  fi
}

thermal_poll() {
  [[ "$TEMP_ENABLE" == "y" ]] || return 0

  local now dt
  now=$(date +%s)
  dt=$(( now - TEMP_LAST_POLL ))
  (( dt < 0 )) && dt=0
  TEMP_LAST_POLL="$now"

  # Refresh cached SMART if needed to read temperature
  maybe_refresh_smart
  if [[ -n "$SMART_LAST_FILE" && -f "$SMART_LAST_FILE" ]]; then
    local t
    t=$(smart_get_temp "$SMART_LAST_FILE" || true)
    if is_number "${t:-}"; then
      TEMP_CUR="$t"
      thermal_update_minmax "$t"
    else
      TEMP_CUR=""
    fi
  fi

  # Nothing else to do if temp not numeric
  if ! is_number "${TEMP_CUR:-}"; then
    return 0
  fi

  # Track time above pause threshold
  if (( TEMP_CUR >= TEMP_PAUSE_C )); then
    TEMP_ABOVE_PAUSE_SECONDS=$(( TEMP_ABOVE_PAUSE_SECONDS + dt ))
  fi

  # Abort threshold
  if (( TEMP_CUR >= TEMP_ABORT_C )); then
    log "Thermal abort: ${TEMP_CUR}C >= ${TEMP_ABORT_C}C"
    return 2
  fi

  # Pause / resume behavior
  if (( TEMP_CUR >= TEMP_PAUSE_C )) && [[ "$CHILD_PAUSED" != "y" ]]; then
    log "Thermal pause: ${TEMP_CUR}C >= ${TEMP_PAUSE_C}C"
    child_pause "$CHILD_PID"
  elif (( TEMP_CUR <= TEMP_RESUME_C )) && [[ "$CHILD_PAUSED" == "y" ]]; then
    log "Thermal resume: ${TEMP_CUR}C <= ${TEMP_RESUME_C}C"
    child_resume "$CHILD_PID"
  fi

  # Accumulate paused time only while actually paused
  if [[ "$CHILD_PAUSED" == "y" ]]; then
    TEMP_PAUSED_SECONDS=$(( TEMP_PAUSED_SECONDS + dt ))
  fi

  # Fail if stayed above pause threshold too long
  if is_number "$TEMP_FAIL_MIN" && (( TEMP_FAIL_MIN > 0 )); then
    local limit=$(( TEMP_FAIL_MIN * 60 ))
    if (( TEMP_ABOVE_PAUSE_SECONDS >= limit )); then
      log "Thermal fail: above-pause for ${TEMP_ABOVE_PAUSE_SECONDS}s (limit ${limit}s)"
      return 3
    fi
  fi

  return 0
}

# -----------------------------
# SMART caching + rendering
# -----------------------------
maybe_refresh_smart() {
  local now
  now=$(date +%s)
  if [[ -z "$SMART_LAST_FILE" ]]; then
    SMART_LAST_FILE="${PC_TMP_DIR}/smart_${DISK_SERIAL}_last.txt"
  fi
  if (( SMART_LAST_AT == 0 || now - SMART_LAST_AT >= SMART_REFRESH_S )); then
    smart_snapshot "$SMART_LAST_FILE"
    SMART_LAST_AT="$now"
  fi
}

smart_init_snapshots() {
  SMART_INITIAL_FILE="${PC_TMP_DIR}/smart_${DISK_SERIAL}_initial.txt"
  SMART_LAST_FILE="${PC_TMP_DIR}/smart_${DISK_SERIAL}_last.txt"
  smart_snapshot "$SMART_INITIAL_FILE"
  cp -f "$SMART_INITIAL_FILE" "$SMART_LAST_FILE" 2>/dev/null || true
  SMART_LAST_AT=$(date +%s)
}

smart_render_box() {
  local width=$UI_WIDTH

  local initial_realloc initial_pend initial_unc initial_crc initial_hours initial_temp
  local current_realloc current_pend current_unc current_crc current_hours current_temp

  if [[ -s "$SMART_INITIAL_FILE" && -s "$SMART_LAST_FILE" ]]; then
    initial_realloc=$(smart_extract_attr "$SMART_INITIAL_FILE" Reallocated_Sector_Ct)
    initial_pend=$(smart_extract_attr "$SMART_INITIAL_FILE" Current_Pending_Sector)
    initial_unc=$(smart_extract_attr "$SMART_INITIAL_FILE" Offline_Uncorrectable)
    initial_crc=$(smart_extract_attr "$SMART_INITIAL_FILE" UDMA_CRC_Error_Count)
    initial_hours=$(smart_extract_attr "$SMART_INITIAL_FILE" Power_On_Hours)
    initial_temp=$(smart_get_temp "$SMART_INITIAL_FILE")

    current_realloc=$(smart_extract_attr "$SMART_LAST_FILE" Reallocated_Sector_Ct)
    current_pend=$(smart_extract_attr "$SMART_LAST_FILE" Current_Pending_Sector)
    current_unc=$(smart_extract_attr "$SMART_LAST_FILE" Offline_Uncorrectable)
    current_crc=$(smart_extract_attr "$SMART_LAST_FILE" UDMA_CRC_Error_Count)
    current_hours=$(smart_extract_attr "$SMART_LAST_FILE" Power_On_Hours)
    current_temp=$(smart_get_temp "$SMART_LAST_FILE")
  else
    initial_realloc=""; initial_pend=""; initial_unc=""; initial_crc=""; initial_hours=""; initial_temp=""
    current_realloc=""; current_pend=""; current_unc=""; current_crc=""; current_hours=""; current_temp=""
  fi

  box_line
  pad_center "S.M.A.R.T. Status (device type: ${SMART_TYPE})"
  pad_lr "" ""

  # Header
  pad_lr "ATTRIBUTE" "INITIAL   CURRENT   STATUS"

  smart_attr_line "Reallocated_Sector_Ct" "$initial_realloc" "$current_realloc"
  smart_attr_line "Power_On_Hours" "$initial_hours" "$current_hours"
  smart_attr_line "Temperature_Celsius" "$initial_temp" "$current_temp"
  smart_attr_line "Current_Pending_Sector" "$initial_pend" "$current_pend"
  smart_attr_line "Offline_Uncorrectable" "$initial_unc" "$current_unc"
  smart_attr_line "UDMA_CRC_Error_Count" "$initial_crc" "$current_crc"

  pad_lr "" ""

  # Overall health (best-effort)
  local health="UNKNOWN"
  if [[ -s "$SMART_LAST_FILE" ]]; then
    if grep -q "SMART overall-health self-assessment test result: PASSED" "$SMART_LAST_FILE"; then
      health="PASSED"
    elif grep -q "SMART overall-health self-assessment test result:" "$SMART_LAST_FILE"; then
      health=$(grep -m1 "SMART overall-health self-assessment test result:" "$SMART_LAST_FILE" | awk -F: '{print $2}' | xargs)
      [[ -z "$health" ]] && health="UNKNOWN"
    fi
  fi

  pad_lr "SMART overall-health self-assessment test result: ${health}" ""
  box_line
}

smart_attr_line() {
  local name="$1"; local init="$2"; local cur="$3"
  local status="-"

  # Normalize empties
  [[ -z "$init" ]] && init="-"
  [[ -z "$cur" ]] && cur="-"

  # Status up/down only if both numeric
  if is_number "${init:-}" && is_number "${cur:-}"; then
    if (( cur > init )); then status="Up $((cur-init))";
    elif (( cur < init )); then status="Down $((init-cur))";
    else status="-";
    fi
  fi

  # Left and right formatting
  local left
  left=$(printf '%-24s' "$name")
  local right
  right=$(printf '%-8s %-8s %-10s' "$init" "$cur" "$status")
  pad_lr "$left" "$right"
}

# -----------------------------
# UI rendering (classic layout)
# -----------------------------
render_ui() {
  local step_line
  local disk_temp_display

  if is_number "${TEMP_CUR:-}"; then
    disk_temp_display="${TEMP_CUR}C"
  else
    disk_temp_display="n/aC"
  fi

  clear_screen

  box_line
  pad_lr "" ""
  pad_center "Preclear-NG Pipeline (${PLATFORM_NAME} / universal) - ${DISK} (${DISK_SERIAL})"
  pad_center "Cycle ${CUR_CYCLE} of ${CYCLES}" 
  pad_lr "" ""

  step_line="Step ${STEP_NUM} of 6 - ${STEP_NAME}:"

  # progress right text
  local right="(${PERCENT}% Done)"
  pad_lr "  ${step_line}" "${right}"

  pad_lr "" ""
  pad_lr "  Disk Temp: ${disk_temp_display} (step min ${TEMP_STEP_MIN:-?} / ${TEMP_STEP_MAX:-?})  above-pause $(hr_time "$TEMP_ABOVE_PAUSE_SECONDS") (total)  paused $(hr_time "$TEMP_PAUSED_SECONDS")  pause ${TEMP_PAUSE_C}C resume ${TEMP_RESUME_C}C abort ${TEMP_ABORT_C}C fail>${TEMP_FAIL_MIN}m" ""

  pad_lr "" ""

  local elapsed_step=$(( $(date +%s) - STEP_STARTED_AT ))
  local elapsed_total=$(( $(date +%s) - TOTAL_STARTED_AT ))

  local cur_speed_display="${CUR_SPEED:-?}"
  local avg_speed_display="${AVG_SPEED:-?}"

  pad_lr "  ** Time elapsed: $(hr_time "$elapsed_step") | Current speed: ${cur_speed_display} | Average speed: ${avg_speed_display}" ""
  box_line

  pad_lr "  Cycle elapsed time: $(hr_time "$elapsed_total") | Total elapsed time: $(hr_time "$elapsed_total")" ""
  box_line

  # SMART box pinned at bottom
  smart_render_box
}

# -----------------------------
# Progress parsing (dd)
# -----------------------------
start_dd_with_progress() {
  # Args: mode(read|write), cmd array via global
  local mode="$1"
  local cmd_str="$2"
  local progress_fifo="$3"

  rm -f "$progress_fifo" 2>/dev/null || true
  mkfifo "$progress_fifo"

  # Parser in background
  (
    local line
    while IFS= read -r line; do
      # dd status=progress line contains "bytes" and "copied"
      # Example: "95189729280 bytes (95 GB, 89 GiB) copied, 892 s, 107 MB/s"
      if [[ "$line" == *"bytes"*"copied"* ]]; then
        local bytes speed
        bytes=$(echo "$line" | awk '{print $1}' 2>/dev/null || true)
        speed=$(echo "$line" | awk -F', ' '{print $3}' 2>/dev/null || true)
        if is_number "${bytes:-}"; then
          CUR_BYTES="$bytes"
        fi
        if [[ -n "${speed:-}" ]]; then
          CUR_SPEED="$speed"
        fi
      fi
    done < "$progress_fifo"
  ) &
  local parser_pid=$!

  # Run dd, redirect stderr into fifo
  # Use subshell to ensure fifo is closed when dd exits
  (
    eval "$cmd_str" 2> "$progress_fifo"
  ) &
  CHILD_PID=$!
  CHILD_KIND="dd"

  # Ensure fifo cleanup on exit of dd
  (
    wait "$CHILD_PID" || true
    # Close fifo by removing it
    rm -f "$progress_fifo" 2>/dev/null || true
    kill "$parser_pid" 2>/dev/null || true
  ) &
}

dd_wait_and_monitor() {
  local total_bytes="$1"
  local pid="$CHILD_PID"

  CHILD_LAST_PROGRESS_AT=$(date +%s)
  CHILD_LAST_BYTES="$CUR_BYTES"

  while kill -0 "$pid" 2>/dev/null; do
    # compute percent and avg speed
    if is_number "${CUR_BYTES:-}" && (( total_bytes > 0 )); then
      PERCENT=$(( (CUR_BYTES * 100) / total_bytes ))
      if (( PERCENT > 100 )); then PERCENT=100; fi
    fi

    local now
    now=$(date +%s)
    local elapsed=$(( now - STEP_STARTED_AT ))
    if (( elapsed > 0 )) && is_number "${CUR_BYTES:-}"; then
      # bytes/sec -> MB/s
      local bps=$(( CUR_BYTES / elapsed ))
      AVG_SPEED=$(awk -v bps="$bps" 'BEGIN{printf "%.0f MB/s", bps/1024/1024}')
    fi

    # thermal
    if [[ "$TEMP_ENABLE" == "y" ]]; then
      thermal_poll
      local rc=$?
      if [[ "$rc" != "0" ]]; then
        dd_escalate_stop "$pid" "thermal"
        wait "$pid" 2>/dev/null || true
        return "$rc"
      fi
    fi

    # hang detection (only when not paused)
    if [[ "$CHILD_PAUSED" != "y" ]]; then
      if is_number "${CUR_BYTES:-}" && (( CUR_BYTES == CHILD_LAST_BYTES )); then
        local stalled=$(( now - CHILD_LAST_PROGRESS_AT ))
        if (( stalled >= DD_HANG_KILL_S )); then
          log "dd hang detected (${stalled}s no progress), killing"
          dd_escalate_stop "$pid" "hang"
          wait "$pid" 2>/dev/null || true
          return 4
        elif (( stalled >= DD_HANG_WARN_S )); then
          log "dd stall warning (${stalled}s no progress)"
        fi
      else
        CHILD_LAST_BYTES="$CUR_BYTES"
        CHILD_LAST_PROGRESS_AT="$now"
      fi
    fi

    render_ui
    sleep "$REFRESH_S"
  done

  # Process exited; collect status
  wait "$pid"
}

dd_escalate_stop() {
  local pid="$1"; local why="$2"
  [[ "$pid" -gt 0 ]] || return 0
  if ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  log "Stopping dd (reason=${why})"
  kill -TERM "$pid" 2>/dev/null || true
  sleep 2
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
}

# -----------------------------
# badblocks runner
# -----------------------------
badblocks_run_patterns() {
  local total_bytes="$1"

  local bs="$BB_BLOCKSIZE"
  if [[ -z "$bs" ]]; then
    bs="$DISK_SECTOR_BYTES"
  fi

  # Parse patterns list
  IFS=',' read -r -a patterns <<< "$BB_PATTERNS"

  local pat_index=0
  local pat_total=${#patterns[@]}
  (( pat_total == 0 )) && return 0

  for pat in "${patterns[@]}"; do
    pat=$(echo "$pat" | xargs)
    ((pat_index++))

    # badblocks progress will be approximate; reset bytes and percent per pattern
    CUR_BYTES=0
    PERCENT=0
    CUR_SPEED=""
    AVG_SPEED=""

    # FIFO to capture stderr progress
    local fifo="${PC_TMP_DIR}/bb_${DISK_SERIAL}.fifo"
    rm -f "$fifo" 2>/dev/null || true
    mkfifo "$fifo"

    (
      local line
      while IFS= read -r line; do
        # badblocks -s uses carriage returns; we may get fragments; try to extract percent
        if [[ "$line" =~ ([0-9]{1,3})% ]]; then
          local p=${BASH_REMATCH[1]}
          if is_number "$p"; then
            PERCENT="$p"
          fi
        fi
      done < "$fifo"
    ) &
    local parser_pid=$!

    # Run badblocks destructive write/read pass for this pattern
    # -w: destructive write-test
    # -s: show progress
    # -v: verbose
    # -b: block size
    # -t: test pattern
    # We do exactly 1 pass per pattern.
    (
      badblocks -wsv -b "$bs" -t "$pat" "$DISK" 2> "$fifo"
    ) &
    CHILD_PID=$!
    CHILD_KIND="badblocks"

    thermal_init_for_step

    while kill -0 "$CHILD_PID" 2>/dev/null; do
      # thermal
      if [[ "$TEMP_ENABLE" == "y" ]]; then
        thermal_poll
        local rc=$?
        if [[ "$rc" != "0" ]]; then
          log "Stopping badblocks (thermal rc=${rc})"
          kill -TERM "$CHILD_PID" 2>/dev/null || true
          sleep 2
          kill -KILL "$CHILD_PID" 2>/dev/null || true
          wait "$CHILD_PID" 2>/dev/null || true
          kill "$parser_pid" 2>/dev/null || true
          rm -f "$fifo" 2>/dev/null || true
          return "$rc"
        fi
      fi

      # UI (pattern context goes in STEP_NAME)
      render_ui
      sleep "$REFRESH_S"
    done

    wait "$CHILD_PID"
    local rc=$?

    kill "$parser_pid" 2>/dev/null || true
    rm -f "$fifo" 2>/dev/null || true

    if [[ "$rc" != "0" ]]; then
      log "badblocks failed for pattern ${pat} (rc=${rc})"
      return "$rc"
    fi

    # Reset pause state between patterns
    CHILD_PAUSED="n"
  done

  return 0
}

# -----------------------------
# Optional extras
# -----------------------------
maybe_hdparm_tune() {
  # Best-effort: reduce head parking / spindown interactions during long tests.
  # No failure if hdparm missing.
  command -v hdparm >/dev/null 2>&1 || return 0
  # Disable standby timer (if supported)
  hdparm -S 0 "$DISK" >/dev/null 2>&1 || true
}

maybe_fio_probe() {
  command -v fio >/dev/null 2>&1 || return 0
  # Quick read latency probe (non-destructive): 10s random read
  log "fio latency probe (10s)"
  fio --name=preclear_ng_probe --filename="$DISK" --direct=1 --rw=randread --bs=4k --iodepth=16 --time_based --runtime=10 --numjobs=1 --group_reporting >/dev/null 2>&1 || true
}

maybe_smart_long_test() {
  [[ "$SMART_LONG" == "y" ]] || return 0
  log "Starting SMART long test"
  local type_args
  type_args="$(smartctl_type_arg)"
  # shellcheck disable=SC2086
  smartctl -t long ${type_args} "$DISK" >/dev/null 2>&1 || true
}

# -----------------------------
# Steps
# -----------------------------
CUR_CYCLE=1

step_preread() {
  STEP_NUM=1
  STEP_NAME="Pre-read full surface scan"
  STEP_STARTED_AT=$(date +%s)
  thermal_init_for_step
  CUR_BYTES=0; CUR_SPEED=""; AVG_SPEED=""; PERCENT=0

  local fifo="${PC_TMP_DIR}/dd_${DISK_SERIAL}_preread.fifo"

  # Build dd command string (direct read preferred)
  local cmd="dd if=${DISK} of=/dev/null bs=${DD_BS_READ} status=progress iflag=direct"
  log "RUN: $cmd"

  start_dd_with_progress "read" "$cmd" "$fifo"

  if dd_wait_and_monitor "$DISK_SIZE_BYTES"; then
    return 0
  else
    local rc=$?
    # If invalid argument (direct unsupported), retry buffered
    if [[ "$rc" -ne 0 ]]; then
      log "Pre-read failed rc=${rc}; retrying without iflag=direct"
      cmd="dd if=${DISK} of=/dev/null bs=${DD_BS_READ} status=progress"
      start_dd_with_progress "read" "$cmd" "$fifo"
      dd_wait_and_monitor "$DISK_SIZE_BYTES"
      return $?
    fi
  fi
}

step_badblocks() {
  STEP_NUM=2
  STEP_NAME="Badblocks destructive patterns"
  STEP_STARTED_AT=$(date +%s)
  thermal_init_for_step
  CUR_BYTES=0; CUR_SPEED=""; AVG_SPEED=""; PERCENT=0

  badblocks_run_patterns "$DISK_SIZE_BYTES"
}

step_zero() {
  STEP_NUM=3
  STEP_NAME="Zero fill (write /dev/zero)"
  STEP_STARTED_AT=$(date +%s)
  thermal_init_for_step
  CUR_BYTES=0; CUR_SPEED=""; AVG_SPEED=""; PERCENT=0

  local fifo="${PC_TMP_DIR}/dd_${DISK_SERIAL}_zero.fifo"

  local cmd="dd if=/dev/zero of=${DISK} bs=${DD_BS_WRITE} status=progress oflag=direct conv=fsync"
  log "RUN: $cmd"

  start_dd_with_progress "write" "$cmd" "$fifo"

  if dd_wait_and_monitor "$DISK_SIZE_BYTES"; then
    return 0
  else
    local rc=$?
    log "Zero fill failed rc=${rc}; retrying without oflag=direct"
    cmd="dd if=/dev/zero of=${DISK} bs=${DD_BS_WRITE} status=progress conv=fsync"
    start_dd_with_progress "write" "$cmd" "$fifo"
    dd_wait_and_monitor "$DISK_SIZE_BYTES"
  fi
}

step_postread() {
  STEP_NUM=4
  STEP_NAME="Post-read full surface scan"
  STEP_STARTED_AT=$(date +%s)
  thermal_init_for_step
  CUR_BYTES=0; CUR_SPEED=""; AVG_SPEED=""; PERCENT=0

  local fifo="${PC_TMP_DIR}/dd_${DISK_SERIAL}_postread.fifo"

  local cmd="dd if=${DISK} of=/dev/null bs=${DD_BS_READ} status=progress iflag=direct"
  log "RUN: $cmd"

  start_dd_with_progress "read" "$cmd" "$fifo"

  if dd_wait_and_monitor "$DISK_SIZE_BYTES"; then
    return 0
  else
    local rc=$?
    log "Post-read failed rc=${rc}; retrying without iflag=direct"
    cmd="dd if=${DISK} of=/dev/null bs=${DD_BS_READ} status=progress"
    start_dd_with_progress "read" "$cmd" "$fifo"
    dd_wait_and_monitor "$DISK_SIZE_BYTES"
  fi
}

step_smart_finalize() {
  STEP_NUM=5
  STEP_NAME="SMART final snapshot & deltas"
  STEP_STARTED_AT=$(date +%s)
  thermal_init_for_step
  PERCENT=100

  # Refresh SMART now
  smart_snapshot "$SMART_LAST_FILE"
  SMART_LAST_AT=$(date +%s)

  # render a couple times for visibility
  render_ui
  sleep 2
  render_ui

  return 0
}

step_certificate() {
  STEP_NUM=6
  STEP_NAME="Certificate / report"
  STEP_STARTED_AT=$(date +%s)
  PERCENT=100

  CERT_FILE="${PC_REPORT_DIR}/preclear-ng_certificate_${DISK_SERIAL}_$(date +%Y.%m.%d_%H.%M.%S).txt"

  {
    echo "Preclear-NG Pipeline Certificate"
    echo "==============================="
    echo "Date: $(date)"
    echo "Disk: ${DISK}"
    echo "Model: ${DISK_MODEL}"
    echo "Serial: ${DISK_SERIAL}"
    echo "Size: $(human_bytes "$DISK_SIZE_BYTES")"
    echo "Sector size: ${DISK_SECTOR_BYTES}"
    echo "Rotational: ${DISK_ROTA}"
    echo ""
    echo "Thermal:"
    echo "  temp_min=${TEMP_MIN:-n/a}C temp_max=${TEMP_MAX:-n/a}C"
    echo "  paused_time=$(hr_time "$TEMP_PAUSED_SECONDS")"
    echo "  above_pause_time=$(hr_time "$TEMP_ABOVE_PAUSE_SECONDS") (limit ${TEMP_FAIL_MIN}m)"
    echo "  thresholds: pause=${TEMP_PAUSE_C}C resume=${TEMP_RESUME_C}C abort=${TEMP_ABORT_C}C"
    echo ""
    echo "SMART initial and final snapshots saved:" 
    echo "  initial: ${SMART_INITIAL_FILE}"
    echo "  final:   ${SMART_LAST_FILE}"
    echo ""
    echo "SMART (final) excerpt:"
    echo "----------------------"
    if [[ -s "$SMART_LAST_FILE" ]]; then
      grep -E "SMART overall-health|^(190|194|5|9|187|197|198|199)[[:space:]]" "$SMART_LAST_FILE" || true
    else
      echo "(SMART unavailable)"
    fi
  } > "$CERT_FILE"

  log "Certificate: $CERT_FILE"

  render_ui
  echo ""
  echo "NG pipeline completed. Certificate: $CERT_FILE"
}

# -----------------------------
# Argument parsing
# -----------------------------
usage() {
  cat <<EOF
Usage: sudo $0 [options] /dev/sdX

Options:
  --help                   Show this help
  --version                Print version
  -l, --list               List candidate disks (not mounted)
  -j, --no-prompt           Do not prompt (DANGEROUS)
  --cycles N               Number of cycles (default 1)
  --resume-ng              Resume from last saved step boundary

  --skip-preread           Skip step 1
  --skip-badblocks         Skip step 2
  --skip-zero              Skip step 3
  --skip-postread          Skip step 4

  --badblocks-patterns CSV Default: 0xaa,0x55,0xff,0x00
  --badblocks-blocksize N  Override badblocks block size (default: logical sector)

  --smart-type TYPE        auto|sat|scsi|ata|nvme (default auto)
  --smart-long             Start SMART long test (non-blocking)

  --temp-disable           Disable temperature monitoring
  --temp-pause C           Pause threshold (C)
  --temp-resume C          Resume threshold (C)
  --temp-abort C           Abort threshold (C)
  --temp-interval S        Temperature poll interval (seconds)
  --temp-fail-min M        Fail if above pause for M minutes (default 10)

EOF
}

parse_args() {
  local argv=("$@")
  while (( $# )); do
    case "$1" in
      --help|-h) usage; exit 0;;
      --version) echo "$0 version: $VERSION"; exit 0;;
      -l|--list) list_candidates; exit 0;;
      -j|--no-prompt) NO_PROMPT="y"; shift;;
      --cycles) CYCLES="${2:-}"; shift 2;;
      --resume-ng) RESUME="y"; shift;;

      --skip-preread) SKIP_PREREAD="y"; shift;;
      --skip-badblocks) SKIP_BADBLOCKS="y"; shift;;
      --skip-zero) SKIP_ZERO="y"; shift;;
      --skip-postread) SKIP_POSTREAD="y"; shift;;

      --badblocks-patterns) BB_PATTERNS="${2:-}"; shift 2;;
      --badblocks-blocksize) BB_BLOCKSIZE="${2:-}"; shift 2;;

      --smart-type) SMART_TYPE="${2:-}"; shift 2;;
      --smart-long) SMART_LONG="y"; shift;;

      --temp-disable) TEMP_ENABLE="n"; shift;;
      --temp-pause) TEMP_PAUSE_C="${2:-}"; shift 2;;
      --temp-resume) TEMP_RESUME_C="${2:-}"; shift 2;;
      --temp-abort) TEMP_ABORT_C="${2:-}"; shift 2;;
      --temp-interval) TEMP_POLL_S="${2:-}"; shift 2;;
      --temp-fail-min) TEMP_FAIL_MIN="${2:-}"; shift 2;;

      --) shift; break;;
      -*) die "Unknown option: $1";;
      *)
        # disk
        if [[ -z "$DISK" ]]; then
          DISK="$1"
          shift
        else
          die "Unexpected argument: $1"
        fi
        ;;
    esac
  done

  # If disk not set and leftover args exist
  if [[ -z "$DISK" && $# -gt 0 ]]; then
    DISK="$1"
  fi

  # Validate numeric args
  is_number "$CYCLES" || die "--cycles must be numeric"
  (( CYCLES >= 1 )) || die "--cycles must be >= 1"

  if [[ -n "$BB_BLOCKSIZE" ]]; then
    is_number "$BB_BLOCKSIZE" || die "--badblocks-blocksize must be numeric"
  fi

  if [[ "$TEMP_ENABLE" == "y" ]]; then
    is_number "$TEMP_PAUSE_C" || die "--temp-pause must be numeric"
    is_number "$TEMP_RESUME_C" || die "--temp-resume must be numeric"
    is_number "$TEMP_ABORT_C" || die "--temp-abort must be numeric"
    is_number "$TEMP_POLL_S" || die "--temp-interval must be numeric"
    is_number "$TEMP_FAIL_MIN" || die "--temp-fail-min must be numeric"
  fi
}

# -----------------------------
# Main
# -----------------------------
main() {
  parse_args "$@"

  [[ -n "$DISK" ]] || die "Disk not set. Use --list to see candidates."
  [[ -b "$DISK" ]] || die "Not a block device: $DISK"

  is_root || die "Must run as root"
  require_deps

  if ! is_preclear_candidate "$DISK"; then
    die "Refusing to run on $DISK (mounted or root disk)."
  fi

  detect_disk_identity
  STATE_FILE="${PC_PLUGIN_DIR}/${DISK_SERIAL}.ng.state"

  log "Starting Preclear-NG unified pipeline on $DISK (serial=$DISK_SERIAL model=$DISK_MODEL)"

  smart_init_snapshots

  # resume
  if [[ "$RESUME" == "y" ]]; then
    if read_state; then
      log "Resume enabled: starting from step $START_STEP"
    else
      log "Resume requested, but no valid state file found; starting at step 1"
      START_STEP=1
    fi
  fi

  # Ensure thresholds for drive type
  # (detect_disk_identity already set HDD/SSD defaults)

  confirm_destruction

  TOTAL_STARTED_AT=$(date +%s)
  maybe_hdparm_tune
  maybe_fio_probe

  local cycle
  for cycle in $(seq 1 "$CYCLES"); do
    CUR_CYCLE="$cycle"

    # Step 1
    if (( START_STEP <= 1 )) && [[ "$SKIP_PREREAD" != "y" ]]; then
      write_state 1 "$cycle"
      step_preread
    fi

    # Step 2
    if (( START_STEP <= 2 )) && [[ "$SKIP_BADBLOCKS" != "y" ]]; then
      write_state 2 "$cycle"
      step_badblocks
    fi

    # Step 3
    if (( START_STEP <= 3 )) && [[ "$SKIP_ZERO" != "y" ]]; then
      write_state 3 "$cycle"
      step_zero
    fi

    # Step 4
    if (( START_STEP <= 4 )) && [[ "$SKIP_POSTREAD" != "y" ]]; then
      write_state 4 "$cycle"
      step_postread
    fi

    # Step 5
    if (( START_STEP <= 5 )); then
      write_state 5 "$cycle"
      maybe_smart_long_test
      step_smart_finalize
    fi

    # Step 6
    if (( START_STEP <= 6 )); then
      write_state 6 "$cycle"
      step_certificate
    fi

    # Next cycle always starts at step 1
    START_STEP=1
  done

  # Cleanup state
  rm -f "$STATE_FILE" 2>/dev/null || true
  return 0
}

main "$@"
