#!/usr/bin/env bash
# Pi-PreClear NG (unified pipeline)
# - Single pipeline (NG) only. Legacy pipeline removed.
# - Designed for Raspberry Pi / Linux hosts.
# - Steps: pre-read -> badblocks destructive (optional) -> smart long (optional) -> zero write -> final read -> smart delta + certificate
#
# NOTE: This script DESTRUCTIVELY tests/overwrites the target disk.

set -Eeuo pipefail

VERSION="1.0.22-ng-unified"

# -------- defaults --------
CYCLES=1
START_STEP=1
RESUME_NG="n"
NO_PROMPT="n"

# Thermal thresholds (C)
TEMP_POLL_S=10
PAUSE_C=50
RESUME_C=45
ABORT_C=55

# SMART
SMART_TYPE="auto"          # smartctl -d <type>; "auto" means no -d
SMART_REFRESH_S=120

# I/O settings
DD_BS="4M"
TRY_DIRECT="y"             # try iflag/oflag=direct first (fallback automatically)

# Optional steps
DO_BADBLOCKS="y"
BADBLOCKS_BSZ="512"         # default; will auto-adjust to sector size if needed
BADBLOCKS_PATTERNS=(0xaa 0x55 0xff 0x00)
DO_SMART_LONG="n"
DO_FIO_PROBE="y"
FIO_LATENCY_S=10
DO_HDPARM_TUNE="y"          # disable head-park (best-effort)

# UI
HUD_REFRESH_S=1

# Paths
LOG_FILE="/var/log/preclear.disk.log"
STATE_DIR="/var/lib/preclear-ng/pi-preclear"
TMP_DIR="/tmp/.preclear"
REPORT_DIR_DEFAULT="$HOME/preclear_reports"

# -------- globals (initialized later) --------
DISK=""
DISK_MODEL=""
DISK_SERIAL=""
DISK_BYTES=0

NG_TEMP_CUR="n/a"
NG_TEMP_MIN=""
NG_TEMP_MAX=""
NG_TEMP_STEP_MIN=""
NG_TEMP_STEP_MAX=""
NG_TEMP_ABOVE_PAUSE_S=0
NG_TEMP_PAUSED_S=0
NG_TEMP_PAUSED_AT=0
NG_TEMP_IS_PAUSED="n"

SMART_INITIAL_FILE=""
SMART_BEFORE_FILE=""
SMART_AFTER_FILE=""
CERT_FILE=""
STATE_FILE=""

CHILD_PID=""

# -------- helpers --------
is_num() { [[ "${1:-}" =~ ^[0-9]+$ ]]; }

ts_now() { date '+%b %d %T'; }

log() {
  local msg="$*"
  local ts
  ts=$(ts_now)
  printf '%s preclear-ng: %s\n' "$ts" "$msg" | tee -a "$LOG_FILE" >/dev/null || true
}

# Backward-compat alias used by older NG codepaths
ng_log_line() { log "$@"; }

warn() { echo "WARNING: $*" >&2; log "WARN: $*"; }

die() {
  echo "ERROR: $*" >&2
  log "FATAL: $*"
  exit 1
}

human_bytes() {
  local b=${1:-0}
  awk -v b="$b" 'BEGIN{
    split("B KB MB GB TB PB", u, " ");
    i=1;
    while (b>=1024 && i<6){b/=1024; i++}
    if (i==1) printf "%.0f %s", b, u[i];
    else printf "%.1f %s", b, u[i];
  }'
}

smartctl_type_arg() {
  case "$SMART_TYPE" in
    auto|"" ) echo "" ;;
    * ) echo "-d $SMART_TYPE" ;;
  esac
}

smartctl_run() {
  local disk="$1"
  local type_args
  type_args=$(smartctl_type_arg)
  # shellcheck disable=SC2086
  timeout -s 9 30 smartctl --all $type_args "$disk" 2>/dev/null
}

smart_has_info() {
  local disk="$1"
  smartctl_run "$disk" | grep -qiE 'START OF INFORMATION SECTION|SMART (overall-health|Health Status)|SMART Attributes Data Structure'
}

smart_snapshot() {
  local out="$1"
  if smart_has_info "$DISK"; then
    smartctl_run "$DISK" >"$out" || true
  else
    echo "SMART: unavailable (smartctl could not read drive)" >"$out"
  fi
}

smart_temp_c() {
  # Returns: integer temp in C, or "n/a"
  local s
  s=$(smartctl_run "$DISK" 2>/dev/null || true)
  # Common fields:
  #  -194 Temperature_Celsius / Airflow_Temperature_Cel
  #  -Current Drive Temperature:
  #  -Temperature:
  local t
  t=$(awk '
    BEGIN{t=""}
    /Current Drive Temperature:/ {for(i=1;i<=NF;i++){if($(i)~ /^[0-9]+$/){t=$(i); break}}}
    /^[0-9]+[[:space:]]+(Temperature_Celsius|Airflow_Temperature_Cel)[[:space:]]/ {for(i=NF;i>=1;i--){if($(i)~ /^[0-9]+$/){t=$(i); break}}}
    /Temperature:[[:space:]]+[0-9]+/ {for(i=1;i<=NF;i++){if($(i)~ /^[0-9]+$/){t=$(i); break}}}
    END{print t}
  ' <<<"$s")

  if is_num "$t"; then echo "$t"; else echo "n/a"; fi
}

smart_health_line() {
  local s
  s=$(smartctl_run "$DISK" 2>/dev/null || true)
  local h
  h=$(grep -m1 -E 'SMART overall-health self-assessment test result:|SMART Health Status:' <<<"$s" || true)
  [[ -n "$h" ]] && echo "$h" || echo "SMART health: n/a"
}

smart_attr_table() {
  # Print a small, stable attribute table (Unraid-like) if available
  local s
  s=$(smartctl_run "$DISK" 2>/dev/null || true)
  if ! grep -q "^ID#" <<<"$s"; then
    echo "(SMART attributes unavailable)"
    return 0
  fi
  # Pick common attributes; fall back to whatever exists.
  # Format: NAME VALUE
  awk '
    BEGIN{
      want["Reallocated_Sector_Ct"]=1;
      want["Power_On_Hours"]=1;
      want["Runtime_Bad_Block"]=1;
      want["End-to-End_Error"]=1;
      want["Reported_Uncorrect"]=1;
      want["Airflow_Temperature_Cel"]=1;
      want["Temperature_Celsius"]=1;
      want["Reallocated_Event_Count"]=1;
      want["Current_Pending_Sector"]=1;
      want["Offline_Uncorrectable"]=1;
      want["UDMA_CRC_Error_Count"]=1;
    }
    /^ID#/ {in=1; next}
    in && $1 ~ /^[0-9]+$/ {
      name=$2;
      raw=$NF;
      if (want[name]) {
        printf "%-24s %s\n", name, raw;
        seen++;
      }
    }
    END{ if (seen==0) print "(No preferred SMART attrs found)"; }
  ' <<<"$s" | head -n 12
}

state_write() {
  local step="$1" cycle="$2"
  umask 077
  mkdir -p "$STATE_DIR" 2>/dev/null || true
  cat >"$STATE_FILE" <<EOF
step=$step
cycle=$cycle
paused_s=$NG_TEMP_PAUSED_S
above_pause_s=$NG_TEMP_ABOVE_PAUSE_S
EOF
}

state_load() {
  # Secure-ish parse of key=value (no sourcing)
  [[ -f "$STATE_FILE" ]] || return 1
  local k v
  while IFS='=' read -r k v; do
    case "$k" in
      step|cycle|paused_s|above_pause_s)
        if is_num "$v"; then
          case "$k" in
            step) START_STEP="$v";;
            cycle) CYCLES_CUR_START="$v";;
            paused_s) NG_TEMP_PAUSED_S="$v";;
            above_pause_s) NG_TEMP_ABOVE_PAUSE_S="$v";;
          esac
        fi
      ;;
    esac
  done <"$STATE_FILE"
  return 0
}

state_clear() {
  rm -f "$STATE_FILE" 2>/dev/null || true
}

cleanup_child() {
  if [[ -n "${CHILD_PID:-}" ]]; then
    if kill -0 "$CHILD_PID" 2>/dev/null; then
      kill -INT "$CHILD_PID" 2>/dev/null || true
      sleep 1
      kill -KILL "$CHILD_PID" 2>/dev/null || true
    fi
  fi
  # If we left a child paused, resume it before exit
  if [[ "$NG_TEMP_IS_PAUSED" == "y" && -n "${CHILD_PID:-}" ]]; then
    kill -CONT "$CHILD_PID" 2>/dev/null || true
  fi
}

on_exit() {
  local rc=$?
  cleanup_child
  if [[ $rc -ne 0 ]]; then
    log "EXIT (rc=$rc)"
  fi
}
trap on_exit EXIT

thermal_update() {
  local now temp
  now=$(date +%s)
  temp=$(smart_temp_c)
  NG_TEMP_CUR="$temp"
  if is_num "$temp"; then
    # overall
    if [[ -z "${NG_TEMP_MIN}" || "$temp" -lt "$NG_TEMP_MIN" ]]; then NG_TEMP_MIN="$temp"; fi
    if [[ -z "${NG_TEMP_MAX}" || "$temp" -gt "$NG_TEMP_MAX" ]]; then NG_TEMP_MAX="$temp"; fi
    # per-step
    if [[ -z "${NG_TEMP_STEP_MIN}" || "$temp" -lt "$NG_TEMP_STEP_MIN" ]]; then NG_TEMP_STEP_MIN="$temp"; fi
    if [[ -z "${NG_TEMP_STEP_MAX}" || "$temp" -gt "$NG_TEMP_STEP_MAX" ]]; then NG_TEMP_STEP_MAX="$temp"; fi

    if (( temp >= PAUSE_C )); then
      NG_TEMP_ABOVE_PAUSE_S=$((NG_TEMP_ABOVE_PAUSE_S + TEMP_POLL_S))
    fi

    # Pause/resume based on thresholds
    if [[ -n "${CHILD_PID:-}" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
      local state
      state=$(ps -o state= -p "$CHILD_PID" 2>/dev/null | awk '{print $1}' || true)

      if (( temp >= ABORT_C )); then
        ng_log_line "TEMP ABORT: ${temp}C >= abort ${ABORT_C}C"
        # Persist state so user can resume later
        state_write "$STEP_NUM" "$CUR_CYCLE"
        # Stop child quickly
        kill -INT "$CHILD_PID" 2>/dev/null || true
        sleep 1
        kill -KILL "$CHILD_PID" 2>/dev/null || true
        return 75
      fi

      # Pause
      if (( temp >= PAUSE_C )) && [[ "$NG_TEMP_IS_PAUSED" != "y" ]]; then
        ng_log_line "TEMP PAUSE: ${temp}C >= pause ${PAUSE_C}C"
        kill -TSTP "$CHILD_PID" 2>/dev/null || true
        NG_TEMP_IS_PAUSED="y"
        NG_TEMP_PAUSED_AT="$now"
      fi

      # Track paused seconds only while actually paused
      if [[ "$NG_TEMP_IS_PAUSED" == "y" ]]; then
        # If something else already resumed it, sync state
        if [[ "$state" != "T" ]]; then
          NG_TEMP_IS_PAUSED="n"
          NG_TEMP_PAUSED_AT=0
        else
          NG_TEMP_PAUSED_S=$((NG_TEMP_PAUSED_S + TEMP_POLL_S))
        fi
      fi

      # Resume
      if is_num "$temp" && (( temp <= RESUME_C )) && [[ "$NG_TEMP_IS_PAUSED" == "y" ]]; then
        ng_log_line "TEMP RESUME: ${temp}C <= resume ${RESUME_C}C"
        kill -CONT "$CHILD_PID" 2>/dev/null || true
        NG_TEMP_IS_PAUSED="n"
        NG_TEMP_PAUSED_AT=0
      fi
    fi
  fi

  return 0
}

reset_step_thermal() {
  NG_TEMP_STEP_MIN=""
  NG_TEMP_STEP_MAX=""
  NG_TEMP_ABOVE_PAUSE_S=0
  # do not reset total paused time
}

fmt_hms() {
  local s=${1:-0}
  local h=$((s/3600)) m=$(((s%3600)/60)) ss=$((s%60))
  printf '%d:%02d:%02d' "$h" "$m" "$ss"
}

read_proc_bytes() {
  # Return bytes for dd based on mode: read or write
  local pid="$1" mode="$2"
  local f="/proc/$pid/io"
  [[ -r "$f" ]] || { echo ""; return 0; }
  case "$mode" in
    read) awk '/read_bytes:/ {print $2}' "$f" 2>/dev/null || true ;;
    write) awk '/write_bytes:/ {print $2}' "$f" 2>/dev/null || true ;;
    *) echo "" ;;
  esac
}

last_log_lines() {
  if [[ -r "$LOG_FILE" ]]; then
    tail -n 5 "$LOG_FILE" 2>/dev/null | tail -n 3
  fi
}

draw_hud() {
  # args: step_total step_num step_name pct elapsed_s cur_speed avg_speed
  local step_total="$1" step_num="$2" step_name="$3" pct="$4" elapsed_s="$5" cur_sp="$6" avg_sp="$7"

  local disktemp="$NG_TEMP_CUR"
  local step_min="${NG_TEMP_STEP_MIN:-?}"
  local step_max="${NG_TEMP_STEP_MAX:-?}"
  local above_m=$((NG_TEMP_ABOVE_PAUSE_S/60))
  local paused_m=$((NG_TEMP_PAUSED_S/60))

  local w=116
  local line
  line=$(printf '%*s' "$w" '' | tr ' ' '#')

  # Clear screen
  if command -v tput >/dev/null 2>&1; then tput clear || true; else printf '\033c' || true; fi

  echo "$line"
  printf "# %-112s #\n" "Preclear-NG Pipeline (Pi / universal) - $DISK ($DISK_SERIAL)"
  printf "# %-112s #\n" "Step ${step_num}/${step_total} - ${step_name}"
  printf "# %-112s #\n" "Disk Temp: ${disktemp}C (step min ${step_min} / ${step_max})  above-pause ${above_m}m (total ${above_m}m)  paused ${paused_m}m  pause ${PAUSE_C}C resume ${RESUME_C}C abort ${ABORT_C}C"
  echo "$line"

  printf "# %-112s #\n" "Cycle ${CUR_CYCLE}/${CYCLES}  Progress: ${pct}%%  Elapsed: $(fmt_hms "$elapsed_s")  Speed: ${cur_sp}  Avg: ${avg_sp}"
  echo "$line"

  echo "# SMART Summary"
  echo "$line"
  smart_health_line
  echo
  printf "%-24s %s\n" "ATTRIBUTE" "RAW"
  smart_attr_table

  echo
  echo "$line"
  echo "# Last log lines:"
  echo "$line"
  last_log_lines
  echo "$line"
}

confirm_destruction() {
  [[ "$NO_PROMPT" == "y" ]] && return 0
  echo
  echo "WARNING: This will DESTRUCTIVELY test and overwrite: $DISK"
  echo "Model: $DISK_MODEL  Serial: $DISK_SERIAL  Size: $(human_bytes "$DISK_BYTES")"
  echo
  read -r -p "Type YES to continue: " ans
  [[ "$ans" == "YES" ]] || die "User aborted."
}

maybe_hdparm_tune() {
  [[ "$DO_HDPARM_TUNE" == "y" ]] || return 0
  command -v hdparm >/dev/null 2>&1 || return 0
  # Best-effort: disable spindown timer
  hdparm -S 0 "$DISK" >/dev/null 2>&1 || true
}

maybe_fio_probe() {
  [[ "$DO_FIO_PROBE" == "y" ]] || return 0
  command -v fio >/dev/null 2>&1 || return 0
  log "fio latency probe (${FIO_LATENCY_S}s)"
  fio --name=preclear_ng_probe --filename="$DISK" --direct=1 --rw=randread --bs=4k --iodepth=16 --time_based --runtime="$FIO_LATENCY_S" --numjobs=1 --group_reporting >/dev/null 2>&1 || true
}

run_dd_with_monitor() {
  # args: mode(read|write) label dd_args...
  local mode="$1" label="$2"; shift 2

  local start_t now_t elapsed
  local last_bytes=0 last_t=0
  local cur_speed_bps=0 avg_speed_bps=0

  start_t=$(date +%s)
  last_t="$start_t"

  # Spawn dd (no status=progress to avoid SIGPIPE issues)
  CHILD_PID=""
  ( "$@" ) &
  CHILD_PID=$!

  # Monitor loop
  while kill -0 "$CHILD_PID" 2>/dev/null; do
    now_t=$(date +%s)
    elapsed=$((now_t - start_t))

    # thermal check
    if (( (now_t - last_t) >= TEMP_POLL_S )); then
      if ! thermal_update; then
        true
      else
        local trc=$?
        if [[ $trc -eq 75 ]]; then
          wait "$CHILD_PID" 2>/dev/null || true
          return 75
        fi
      fi
    fi

    local bytes
    bytes=$(read_proc_bytes "$CHILD_PID" "$mode")
    if is_num "$bytes"; then
      if (( now_t > last_t )); then
        cur_speed_bps=$(( (bytes - last_bytes) / (now_t - last_t) ))
      fi
      if (( elapsed > 0 )); then
        avg_speed_bps=$(( bytes / elapsed ))
      fi
      last_bytes="$bytes"
      last_t="$now_t"

      local pct=0
      if (( DISK_BYTES > 0 )); then
        pct=$(( (bytes * 100) / DISK_BYTES ))
        if (( pct > 100 )); then pct=100; fi
      fi
      draw_hud 6 "$STEP_NUM" "$STEP_NAME" "$pct" "$elapsed" "$(human_bytes "$cur_speed_bps")/s" "$(human_bytes "$avg_speed_bps")/s"
    else
      draw_hud 6 "$STEP_NUM" "$STEP_NAME" "0" "$elapsed" "n/a" "n/a"
    fi

    sleep "$HUD_REFRESH_S"
  done

  wait "$CHILD_PID" || return $?
  return 0
}

step_preread() {
  STEP_NUM=1
  STEP_NAME="Pre-read full surface scan"
  reset_step_thermal

  log "RUN: dd if=$DISK of=/dev/null bs=$DD_BS"$([[ "$TRY_DIRECT" == "y" ]] && echo " iflag=direct" || true)

  if [[ "$TRY_DIRECT" == "y" ]]; then
    if run_dd_with_monitor read "$STEP_NAME" dd if="$DISK" of=/dev/null bs="$DD_BS" iflag=direct; then
      return 0
    fi
    local rc=$?
    if [[ $rc -eq 75 ]]; then return 75; fi
    log "Pre-read failed rc=$rc; retrying without iflag=direct"
  fi

  run_dd_with_monitor read "$STEP_NAME" dd if="$DISK" of=/dev/null bs="$DD_BS"
}

step_zero() {
  STEP_NUM=4
  STEP_NAME="Full zero write"
  reset_step_thermal

  log "RUN: dd if=/dev/zero of=$DISK bs=$DD_BS"$([[ "$TRY_DIRECT" == "y" ]] && echo " oflag=direct" || true)

  if [[ "$TRY_DIRECT" == "y" ]]; then
    if run_dd_with_monitor write "$STEP_NAME" dd if=/dev/zero of="$DISK" bs="$DD_BS" oflag=direct conv=fsync; then
      return 0
    fi
    local rc=$?
    if [[ $rc -eq 75 ]]; then return 75; fi
    log "Zero write failed rc=$rc; retrying without oflag=direct"
  fi

  run_dd_with_monitor write "$STEP_NAME" dd if=/dev/zero of="$DISK" bs="$DD_BS" conv=fsync
}

step_finalread() {
  STEP_NUM=5
  STEP_NAME="Final full read verify"
  reset_step_thermal

  log "RUN: dd if=$DISK of=/dev/null bs=$DD_BS"$([[ "$TRY_DIRECT" == "y" ]] && echo " iflag=direct" || true)

  if [[ "$TRY_DIRECT" == "y" ]]; then
    if run_dd_with_monitor read "$STEP_NAME" dd if="$DISK" of=/dev/null bs="$DD_BS" iflag=direct; then
      return 0
    fi
    local rc=$?
    if [[ $rc -eq 75 ]]; then return 75; fi
    log "Final read failed rc=$rc; retrying without iflag=direct"
  fi

  run_dd_with_monitor read "$STEP_NAME" dd if="$DISK" of=/dev/null bs="$DD_BS"
}

monitor_badblocks() {
  # args: pid logfile
  local pid="$1" lf="$2"
  local start_t now_t elapsed pct
  start_t=$(date +%s)

  while kill -0 "$pid" 2>/dev/null; do
    now_t=$(date +%s)
    elapsed=$((now_t - start_t))

    # thermal check
    if (( (now_t - start_t) % TEMP_POLL_S == 0 )); then
      local trc
      thermal_update || trc=$?
      if [[ ${trc:-0} -eq 75 ]]; then
        kill -INT "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        return 75
      fi
    fi

    pct=$(grep -oE '[0-9]+(\.[0-9]+)?% done' "$lf" 2>/dev/null | tail -n 1 | awk '{print $1}' | tr -d '%' || true)
    if ! is_num "${pct%%.*}"; then pct=0; fi

    draw_hud 6 "$STEP_NUM" "$STEP_NAME" "${pct%%.*}" "$elapsed" "n/a" "n/a"
    sleep "$HUD_REFRESH_S"
  done

  wait "$pid" || return $?
  return 0
}

step_badblocks() {
  STEP_NUM=2
  STEP_NAME="badblocks multi-pattern destructive test"
  reset_step_thermal

  command -v badblocks >/dev/null 2>&1 || { warn "badblocks not installed; skipping"; return 0; }

  # Pick a sane block size: at least sector size
  local sector
  sector=$(blockdev --getss "$DISK" 2>/dev/null || echo 512)
  if is_num "$sector" && (( BADBLOCKS_BSZ < sector )); then
    BADBLOCKS_BSZ="$sector"
  fi

  local lf="$TMP_DIR/badblocks_${DISK_SERIAL}.log"
  : >"$lf"

  # Build command
  local cmd=(badblocks -wsv -b "$BADBLOCKS_BSZ")
  local p
  for p in "${BADBLOCKS_PATTERNS[@]}"; do
    cmd+=( -t "$p" )
  done
  cmd+=( "$DISK" )

  log "RUN: ${cmd[*]}"

  ( "${cmd[@]}" ) >"$lf" 2>&1 &
  CHILD_PID=$!

  monitor_badblocks "$CHILD_PID" "$lf"
}

step_smart_long() {
  STEP_NUM=3
  STEP_NAME="SMART long self-test"
  reset_step_thermal

  command -v smartctl >/dev/null 2>&1 || { warn "smartctl not installed; skipping"; return 0; }

  log "RUN: smartctl -t long $(smartctl_type_arg) $DISK"
  # shellcheck disable=SC2046,SC2086
  if ! timeout -s 9 30 smartctl -t long $(smartctl_type_arg) "$DISK" >/dev/null 2>&1; then
    warn "smartctl long-test command failed to start; skipping"
    return 0
  fi

  # Poll every 60s for completion
  local start_t now_t elapsed
  start_t=$(date +%s)
  while :; do
    now_t=$(date +%s)
    elapsed=$((now_t - start_t))

    # thermal check
    thermal_update || true

    local s
    s=$(smartctl_run "$DISK" 2>/dev/null || true)
    # crude completion check
    if grep -qiE 'Self-test routine in progress|in progress' <<<"$s"; then
      draw_hud 6 "$STEP_NUM" "$STEP_NAME" "0" "$elapsed" "n/a" "n/a"
      sleep 60
      continue
    fi
    break
  done

  return 0
}

smart_delta_small() {
  local before="$1" after="$2"
  echo "SMART delta (selected attrs):"
  local tmpb tmpa
  tmpb=$(mktemp)
  tmpa=$(mktemp)
  awk '/^ID#/{in=1;next} in && $1 ~ /^[0-9]+$/ {print $2" "$NF}' "$before" 2>/dev/null | sort >"$tmpb" || true
  awk '/^ID#/{in=1;next} in && $1 ~ /^[0-9]+$/ {print $2" "$NF}' "$after" 2>/dev/null | sort >"$tmpa" || true
  join -a1 -a2 -e "?" -o 0,1.2,2.2 "$tmpb" "$tmpa" 2>/dev/null | awk '{printf "%-24s %10s -> %10s\n", $1, $2, $3}' | head -n 40 || true
  rm -f "$tmpb" "$tmpa" 2>/dev/null || true
}

write_certificate() {
  local result="$1"
  mkdir -p "$REPORT_DIR" 2>/dev/null || true
  {
    echo "Pi-PreClear NG certificate"
    echo "Version: $VERSION"
    echo "Date: $(date -Is)"
    echo "Disk: $DISK"
    echo "Model: $DISK_MODEL"
    echo "Serial: $DISK_SERIAL"
    echo "Size: $(human_bytes "$DISK_BYTES")"
    echo "Result: $result"
    echo
    echo "-- SMART BEFORE --"
    cat "$SMART_BEFORE_FILE" 2>/dev/null || true
    echo
    echo "-- SMART AFTER --"
    cat "$SMART_AFTER_FILE" 2>/dev/null || true
    echo
    smart_delta_small "$SMART_BEFORE_FILE" "$SMART_AFTER_FILE" || true
  } >"$CERT_FILE"
}

usage() {
  cat <<EOF
Usage: sudo $0 [options] /dev/sdX

Options:
  --cycles N                Number of cycles (default: $CYCLES)
  --start-step N            Start at step N (1..6) (default: $START_STEP)
  --resume-ng               Resume from state file (step/cycle) (safe parse)
  --no-prompt               Do not ask to type YES

  --smart-type TYPE         smartctl -d TYPE (auto|sat|scsi|usbjmicron|...) (default: $SMART_TYPE)
  --smart-long              Include SMART long self-test step (step 3)

  --no-badblocks            Skip badblocks destructive step
  --badblocks-bsz N         badblocks block size (default: $BADBLOCKS_BSZ)

  --pause-c N               Pause threshold C (default: $PAUSE_C)
  --resume-c N              Resume threshold C (default: $RESUME_C)
  --abort-c N               Abort threshold C (default: $ABORT_C)

  --no-direct               Do not attempt direct I/O first
  --no-fio                  Skip fio latency probe
  --no-hdparm               Skip hdparm tune

Compatibility:
  --pipeline-ng             Accepted (no-op); pipeline is always NG now.

EOF
}

parse_args() {
  local args=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --cycles) CYCLES="${2:-}"; shift 2;;
      --start-step) START_STEP="${2:-}"; shift 2;;
      --resume-ng) RESUME_NG="y"; shift;;
      --no-prompt) NO_PROMPT="y"; shift;;

      --smart-type) SMART_TYPE="${2:-}"; shift 2;;
      --smart-long) DO_SMART_LONG="y"; shift;;

      --no-badblocks) DO_BADBLOCKS="n"; shift;;
      --badblocks-bsz) BADBLOCKS_BSZ="${2:-}"; shift 2;;

      --pause-c) PAUSE_C="${2:-}"; shift 2;;
      --resume-c) RESUME_C="${2:-}"; shift 2;;
      --abort-c) ABORT_C="${2:-}"; shift 2;;

      --no-direct) TRY_DIRECT="n"; shift;;
      --no-fio) DO_FIO_PROBE="n"; shift;;
      --no-hdparm) DO_HDPARM_TUNE="n"; shift;;

      --pipeline-ng) shift;;
      -h|--help) usage; exit 0;;
      -*) die "Unknown option: $1";;
      *) args+=("$1"); shift;;
    esac
  done

  if [[ ${#args[@]} -ne 1 ]]; then
    usage
    die "Must provide exactly one disk device (e.g., /dev/sda)."
  fi
  DISK="${args[0]}"

  is_num "$CYCLES" || die "--cycles must be numeric"
  is_num "$START_STEP" || die "--start-step must be numeric"
  (( START_STEP >= 1 && START_STEP <= 6 )) || die "--start-step must be 1..6"
}

init_disk_info() {
  [[ -b "$DISK" ]] || die "Not a block device: $DISK."

  DISK_BYTES=$(blockdev --getsize64 "$DISK" 2>/dev/null || echo 0)
  is_num "$DISK_BYTES" || DISK_BYTES=0
  (( DISK_BYTES > 0 )) || die "Could not determine disk size for $DISK"

  mkdir -p "$TMP_DIR" 2>/dev/null || true
  mkdir -p "$REPORT_DIR_DEFAULT" 2>/dev/null || true

  # Best-effort identification
  DISK_MODEL=$(lsblk -ndo MODEL "$DISK" 2>/dev/null | tr ' ' '_' | tr -cd 'A-Za-z0-9._-')
  DISK_SERIAL=$(lsblk -ndo SERIAL "$DISK" 2>/dev/null | tr -cd 'A-Za-z0-9._-')

  # Fallbacks
  [[ -n "$DISK_MODEL" ]] || DISK_MODEL="UNKNOWN_MODEL"
  [[ -n "$DISK_SERIAL" ]] || DISK_SERIAL="UNKNOWN_SERIAL"

  REPORT_DIR="$REPORT_DIR_DEFAULT"
  CERT_FILE="$REPORT_DIR/preclear-ng_certificate_${DISK_SERIAL}_$(date '+%Y.%m.%d_%H.%M.%S').txt"

  STATE_FILE="$STATE_DIR/${DISK_SERIAL}.ng.state"

  SMART_INITIAL_FILE="$TMP_DIR/smart_${DISK_SERIAL}_initial.txt"
  SMART_BEFORE_FILE="$TMP_DIR/smart_${DISK_SERIAL}_before.txt"
  SMART_AFTER_FILE="$TMP_DIR/smart_${DISK_SERIAL}_after.txt"
}

main() {
  [[ $EUID -eq 0 ]] || die "Please run as root (use sudo)."

  parse_args "$@"
  init_disk_info

  log "Auto-detected SMART type: $SMART_TYPE"
  log "Starting Preclear-NG unified pipeline on $DISK (serial=$DISK_SERIAL model=$DISK_MODEL)"

  if [[ "$RESUME_NG" == "y" ]]; then
    if state_load; then
      log "Resume requested; starting at step=$START_STEP (from state file)"
    else
      log "Resume requested but no valid state file found; starting at step=$START_STEP"
    fi
  fi

  confirm_destruction

  maybe_hdparm_tune
  maybe_fio_probe

  # Snapshot SMART before
  smart_snapshot "$SMART_BEFORE_FILE"

  local cycle
  for cycle in $(seq 1 "$CYCLES"); do
    CUR_CYCLE="$cycle"

    # Persist state at step transitions
    state_write "$START_STEP" "$CUR_CYCLE"

    if (( START_STEP <= 1 )); then
      if ! step_preread; then
        local rc=$?
        if [[ $rc -eq 75 ]]; then echo "TEMP abort triggered. Resume later with --resume-ng"; exit 75; fi
        write_certificate "FAIL (pre-read)"
        die "NG pipeline FAILED (pre-read). Certificate: $CERT_FILE"
      fi
    fi

    if (( START_STEP <= 2 )) && [[ "$DO_BADBLOCKS" == "y" ]]; then
      if ! step_badblocks; then
        local rc=$?
        if [[ $rc -eq 75 ]]; then echo "TEMP abort triggered. Resume later with --resume-ng"; exit 75; fi
        write_certificate "FAIL (badblocks)"
        die "NG pipeline FAILED (badblocks). Certificate: $CERT_FILE"
      fi
    fi

    if (( START_STEP <= 3 )) && [[ "$DO_SMART_LONG" == "y" ]]; then
      if ! step_smart_long; then
        local rc=$?
        if [[ $rc -eq 75 ]]; then echo "TEMP abort triggered. Resume later with --resume-ng"; exit 75; fi
        write_certificate "FAIL (smart long)"
        die "NG pipeline FAILED (SMART long). Certificate: $CERT_FILE"
      fi
    fi

    if (( START_STEP <= 4 )); then
      if ! step_zero; then
        local rc=$?
        if [[ $rc -eq 75 ]]; then echo "TEMP abort triggered. Resume later with --resume-ng"; exit 75; fi
        write_certificate "FAIL (zero write)"
        die "NG pipeline FAILED (zero). Certificate: $CERT_FILE"
      fi
    fi

    if (( START_STEP <= 5 )); then
      if ! step_finalread; then
        local rc=$?
        if [[ $rc -eq 75 ]]; then echo "TEMP abort triggered. Resume later with --resume-ng"; exit 75; fi
        write_certificate "FAIL (final read)"
        die "NG pipeline FAILED (final read). Certificate: $CERT_FILE"
      fi
    fi

    START_STEP=1
  done

  # Snapshot SMART after + certificate
  STEP_NUM=6
  STEP_NAME="Capturing SMART after + generating certificate"
  smart_snapshot "$SMART_AFTER_FILE"
  write_certificate "PASS"
  state_clear

  ng_log_line "PASS: Preclear-NG pipeline finished successfully"
  draw_hud 6 6 "DONE - PASS (certificate generated)" 100 0 "n/a" "n/a"

  echo "NG pipeline PASSED. Certificate: $CERT_FILE"
  echo "Log: $LOG_FILE"
}

main "$@"
