#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, re, shutil, sys
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo  # type: ignore

@dataclass
class SummarySpec: folder_template: str; filename_template: str
@dataclass
class SourceSpec: folder_template: str; glob: Optional[str]=None
@dataclass
class DestSpec: folder_template: str
@dataclass
class GroupSpec: source: SourceSpec; dest: DestSpec
@dataclass
class ArgsSpec: domain: Optional[List[str]]=None; max_diff_hours: Optional[float]=None
@dataclass
class Config:
    time_ref: str; tz: str; history_date: Optional[str]
    summary: SummarySpec; files: Dict[str, GroupSpec]; args: ArgsSpec

def load_config(path: Path) -> Config:
    try:
        text = path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"Config: failed to read {path}: {e}", file=sys.stderr); sys.exit(4)
    try:
        raw=json.loads(text)
    except Exception as e:
        print(f"Config: invalid JSON in {path}: {e}", file=sys.stderr); sys.exit(4)

    time_ref=str(raw.get("time_ref","now")).strip().lower()
    if time_ref not in {"now","history"}:
        print("Config: time_ref must be 'now' or 'history'",file=sys.stderr); sys.exit(4)
    tz=raw.get("tz","Europe/Rome"); history_date=raw.get("history_date")
    if time_ref=="history":
        if not history_date:
            print("Config: history_date required when time_ref == 'history'",file=sys.stderr); sys.exit(4)
        try: datetime.strptime(history_date,"%Y-%m-%d")
        except ValueError:
            print(f"Config: invalid history_date: {history_date}",file=sys.stderr); sys.exit(4)
    else:
        history_date=None

    summ=raw.get("summary",{})
    if not summ or "folder_template" not in summ or "filename_template" not in summ:
        print("Config: 'summary' must include folder_template and filename_template",file=sys.stderr); sys.exit(4)

    files_raw=raw.get("files",{})
    if not isinstance(files_raw,dict) or not files_raw:
        print("Config: 'files' must be non-empty mapping",file=sys.stderr); sys.exit(4)
    files: Dict[str,GroupSpec]={}
    for name,grp in files_raw.items():
        src=grp.get("source",{}); dst=grp.get("dest",{})
        if "folder_template" not in src or "folder_template" not in dst:
            print(f"Config: group '{name}' missing source/dest.folder_template",file=sys.stderr); sys.exit(4)
        files[name]=GroupSpec(SourceSpec(src["folder_template"], src.get("glob")), DestSpec(dst["folder_template"]))

    args_raw=raw.get("args",{})
    domains=args_raw.get("domain",None)
    if domains is not None and (not isinstance(domains,list) or not all(isinstance(x,str) for x in domains)):
        print("Config: args.domain must be list[str] if provided",file=sys.stderr); sys.exit(4)
    m = args_raw.get("max_diff_hours",None)
    if m is not None:
        try: m=float(m)
        except Exception:
            print("Config: args.max_diff_hours must be numeric",file=sys.stderr); sys.exit(4)

    return Config(
        time_ref=time_ref,tz=tz,history_date=history_date,
        summary=SummarySpec(summ["folder_template"],summ["filename_template"]),
        files=files,args=ArgsSpec(domain=domains,max_diff_hours=m)
    )

def resolve_target_date(cfg: Config)->date:
    return datetime.now(ZoneInfo(cfg.tz)).date() if cfg.time_ref=="now" else datetime.strptime(cfg.history_date,"%Y-%m-%d").date()  # type: ignore

def expand_day(t:str,d:date)->str:
    return t.replace("{YYYY}",f"{d.year:04d}").replace("{MM}",f"{d.month:02d}").replace("{DD}",f"{d.day:02d}")

def to_path(t:str,d:date)->Path: return Path(expand_day(t,d))

def filename_glob_from_template(t:str,d:date)->str:
    return t.replace("{YYYY}",f"{d.year:04d}").replace("{MM}",f"{d.month:02d}").replace("{DD}",f"{d.day:02d}").replace("{RUN}","*")

def parse_run_from_filename(tmpl:str,name:str)->Optional[str]:
    p=re.escape(tmpl)
    p=p.replace(r"\{YYYY\}",r"(?P<YYYY>\d{4})").replace(r"\{MM\}",r"(?P<MM>\d{2})").replace(r"\{DD\}",r"(?P<DD>\d{2})").replace(r"\{RUN\}",r"(?P<RUN>\d{4})")
    m=re.fullmatch(p,name); return m.group("RUN") if m else None

def select_newest_summary(cfg:Config,d:date,run_cutoff:Optional[int]=None)->Path:
    folder=to_path(cfg.summary.folder_template,d)
    if not folder.exists():
        print(f"Summary folder not found: {folder}",file=sys.stderr); sys.exit(3)
    glob_pat=filename_glob_from_template(cfg.summary.filename_template,d)
    candidates=sorted(folder.glob(glob_pat))
    if not candidates:
        print(f"No summary files in {folder} matching {glob_pat}",file=sys.stderr); sys.exit(2)
    newest:(Optional[Path],Optional[int])=(None,None)
    for p in candidates:
        run=parse_run_from_filename(cfg.summary.filename_template,p.name)
        if not run: continue
        try: hhmm=int(run)
        except ValueError: continue
        if run_cutoff is not None and hhmm>run_cutoff: continue
        if newest[1] is None or hhmm>newest[1]: newest=(p,hhmm)
    if newest[0] is None:
        print("No valid RUN parsed from filenames",file=sys.stderr); sys.exit(2)
    return newest[0]

def yyyymmddhhmm_to_dt(s:str,tz:str)->datetime:
    return datetime.strptime(s,"%Y%m%d%H%M").replace(tzinfo=ZoneInfo(tz))

def expand_all(t:str,YYYY:str,MM:str,DD:str,RUN:str,DOMAIN:Optional[str])->str:
    return t.replace("{YYYY}",YYYY).replace("{MM}",MM).replace("{DD}",DD).replace("{RUN}",RUN).replace("{DOMAIN}", DOMAIN or "")

def gather_files(base:Path,pattern:Optional[str])->List[Path]:
    if not base.exists(): return []
    return [p for p in (base.glob(pattern) if pattern else base.iterdir()) if p.is_file()]

def parse_hhmm(s:str)->dtime: return datetime.strptime(s,"%H:%M").time()

def dt_from_filename(name:str, regex: Optional[re.Pattern], tz:str)->Optional[datetime]:
    if not regex: return None
    m=regex.search(name)
    if not m: return None
    try:
        if "ts" in m.groupdict():
            return datetime.strptime(m.group("ts"),"%Y%m%d%H%M").replace(tzinfo=ZoneInfo(tz))
        YYYY=int(m.group("YYYY")); MM=int(m.group("MM")); DD=int(m.group("DD"))
        HH=int(m.group("HH")) if "HH" in m.groupdict() else 0
        mm=int(m.group("mm")) if "mm" in m.groupdict() else 0
        return datetime(YYYY,MM,DD,HH,mm,tzinfo=ZoneInfo(tz))
    except Exception:
        return None

def main(argv: Optional[List[str]] = None) -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument("--config",required=True,type=Path)
    ap.add_argument("--limit-to-obs",action="store_true")
    ap.add_argument("--end-at-date-obs",action="store_true")
    ap.add_argument("--dry-run",action="store_true")
    ap.add_argument("--debug-gate",action="store_true", help="include per-file gating diagnostics even when not dry-run")
    ap.add_argument("--when"); ap.add_argument("--date"); ap.add_argument("--time")
    ap.add_argument("--n-days",type=int,default=0); ap.add_argument("--flatten-hour"); ap.add_argument("--start-hour")
    ap.add_argument("--filename-date-regex"); ap.add_argument("--freq-minutes",type=int)
    args=ap.parse_args(argv)

    cfg=load_config(args.config)

    # Allow --when (ISO date or datetime) or --date+--time to force history
    if args.when:
        try:
            dt_when = datetime.fromisoformat(args.when)
            cfg.time_ref = "history"; cfg.history_date = dt_when.date().isoformat()
        except ValueError:
            print(f"--when must be ISO8601 date or datetime: {args.when}", file=sys.stderr); return 4
    elif args.date and args.time:
        cfg.time_ref="history"; cfg.history_date = args.date

    run_cutoff=None
    if args.time: run_cutoff=int(parse_hhmm(args.time).strftime("%H%M"))

    tz=cfg.tz
    now_run = datetime.now(ZoneInfo(tz))  # run time in configured tz

    target_date=resolve_target_date(cfg)
    summary_path=select_newest_summary(cfg,target_date,run_cutoff)
    summary=json.loads(summary_path.read_text(encoding="utf-8"))

    run_date=summary.get("run_date"); run=summary.get("run")
    if not (run_date and run):
        print("summary missing run_date/run",file=sys.stderr); return 4
    YYYY,MM,DD=run_date.split("-"); RUN=run
    me=summary.get("me",{}).get("data",{})
    date_ini, date_end, date_obs = me.get("DateIni"), me.get("DateEnd"), me.get("DateObs")
    if not (date_ini and date_end):
        print("summary missing DateIni/DateEnd",file=sys.stderr); return 4

    limit_to_obs = bool(getattr(args,"limit_to_obs",False) or getattr(args,"end_at_date_obs",False))
    if limit_to_obs and not date_obs:
        print("summary missing DateObs with --limit-to-obs/--end-at-date-obs",file=sys.stderr); return 4

    dt_ini=yyyymmddhhmm_to_dt(date_ini,tz)
    dt_end=yyyymmddhhmm_to_dt(date_obs if limit_to_obs else date_end,tz)

    if args.flatten_hour and cfg.time_ref=="now":
        flat_t=parse_hhmm(args.flatten_hour); ifnow=now_run.time()
        if ifnow < flat_t: dt_ini -= timedelta(days=1)

    if args.n_days>0:
        base_start=(dt_ini.date()-timedelta(days=args.n_days))
        if args.start_hour:
            hhmm=parse_hhmm(args.start_hour); dt_ini=datetime.combine(base_start,hhmm,tzinfo=ZoneInfo(cfg.tz))
        else:
            dt_ini=datetime.combine(base_start,dt_ini.timetz())

    fn_dt_regex = re.compile(args.filename_date_regex) if args.filename_date_regex else None

    info_tz = summary.get("tz") or summary.get("timing", {}).get("tz") or cfg.tz
    oldest_sf_str = summary.get("timing", {}).get("overall_oldest_fs_time")
    oldest_dt: Optional[datetime] = None
    if oldest_sf_str and isinstance(oldest_sf_str, str):
        try:
            oldest_dt = datetime.strptime(oldest_sf_str,"%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo(info_tz)).astimezone(ZoneInfo(tz))
        except Exception:
            oldest_dt = None

    report: Dict[str,Any] = {
        "status":"ok",
        "dry_run": bool(args.dry_run),
        "resolved":{"target_date":target_date.isoformat(),"summary_path":str(summary_path)},
        "run":{"date":run_date,"run":RUN},
        "time_window":{"tz":tz,"start":dt_ini.isoformat(),"end":dt_end.isoformat(),
                       "limit_to_obs":bool(limit_to_obs),"flatten_hour":args.flatten_hour,
                       "n_days":args.n_days,"start_hour":args.start_hour},
        "domain": cfg.args.domain if cfg.args.domain is not None else None,
        "gating":{"oldest_sf_time": oldest_dt.isoformat() if oldest_dt else None,
                  "run_time": now_run.isoformat(),
                  "rule": "if mtime<=oldest→use oldest; else use run_time",
                  "max_diff_hours": cfg.args.max_diff_hours,
                  "tz_info_run": info_tz},
        "groups": {},
        "folders": {"sources": [], "destinations": []}  # NEW aggregate summary
    }

    copied_total=0
    domains: List[Optional[str]] = cfg.args.domain if cfg.args.domain else [None]
    all_sources: set[str] = set()
    all_dests: set[str] = set()

    def gate_decision(file_mtime: datetime, oldest_dt: Optional[datetime], run_time: datetime, max_secs: Optional[float]):
        if max_secs is None:
            return True, "no_gate", None, None
        # If we don't have oldest_dt, compare to run_time only
        if oldest_dt is None:
            diff_rt = (run_time - file_mtime).total_seconds()
            if diff_rt < 0:  # future-dated mtime
                return False, "run_time_future", diff_rt, None
            return (0 <= diff_rt <= max_secs), "run_time", diff_rt, None
        # Piecewise: before oldest → compare to oldest; after oldest → compare to run_time
        if file_mtime <= oldest_dt:
            diff_old = (oldest_dt - file_mtime).total_seconds()
            return (0 <= diff_old <= max_secs), "oldest_sf_time", diff_old, (run_time - file_mtime).total_seconds()
        else:
            diff_rt = (run_time - file_mtime).total_seconds()
            if diff_rt < 0:
                return False, "run_time_future", diff_rt, (oldest_dt - file_mtime).total_seconds()
            return (0 <= diff_rt <= max_secs), "run_time", diff_rt, (oldest_dt - file_mtime).total_seconds()

    for DOMAIN in domains:
        for group_name,spec in cfg.files.items():
            src_folder=Path(expand_all(spec.source.folder_template,YYYY,MM,DD,RUN,DOMAIN))
            dst_folder=Path(expand_all(spec.dest.folder_template,YYYY,MM,DD,RUN,DOMAIN))
            candidates=gather_files(src_folder,spec.source.glob)

            data_times_and_files: List[Tuple[datetime,Path,float]] = []
            for p in candidates:
                st = p.stat()
                data_ts = dt_from_filename(p.name, fn_dt_regex, tz) or datetime.fromtimestamp(st.st_mtime, tz=ZoneInfo(tz))
                if dt_ini <= data_ts <= dt_end:
                    data_times_and_files.append((data_ts,p,st.st_mtime))
            data_times_and_files.sort(key=lambda x:x[0])

            selected: List[Tuple[datetime,Path,float]] = []
            if args.freq_minutes:
                step=args.freq_minutes*60; last_kept: Optional[datetime]=None
                for t,p,mtime in data_times_and_files:
                    if last_kept is None or (t - last_kept).total_seconds() >= step:
                        selected.append((t,p,mtime)); last_kept=t
            else:
                selected=list(data_times_and_files)

            gate_debug_entries = []
            if cfg.args.max_diff_hours is None:
                gated = [p for _,p,_ in selected]
            else:
                max_secs = cfg.args.max_diff_hours * 3600.0
                gated=[]
                sel_map = {p: t for t,p,_ in selected}
                for _, p, mtime in selected:
                    file_mtime = datetime.fromtimestamp(mtime, tz=ZoneInfo(tz))
                    keep, anchor, diff_used, diff_other = gate_decision(file_mtime, oldest_dt, now_run, max_secs)
                    if keep: gated.append(p)
                    if args.dry_run or args.debug_gate:
                        gate_debug_entries.append({
                            "name": p.name,
                            "data_ts": sel_map[p].isoformat(),
                            "file_mtime": file_mtime.isoformat(),
                            "oldest_sf_time": oldest_dt.isoformat() if oldest_dt else None,
                            "run_time": now_run.isoformat(),
                            "anchor": anchor,
                            "diff_hours_used": None if diff_used is None else round(diff_used/3600.0, 3),
                            "diff_hours_other": None if diff_other is None else round(diff_other/3600.0, 3),
                            "max_diff_hours": cfg.args.max_diff_hours,
                            "decision": "copy" if keep else "skip"
                        })

            ops=[]
            for p in gated:
                target=dst_folder / p.name
                # record both folders explicitly (NEW)
                ops.append({
                    "from": str(p),
                    "to": str(target),
                    "source_folder": str(p.parent),
                    "dest_folder": str(dst_folder),
                    "name": p.name
                })
                if not args.dry_run:
                    dst_folder.mkdir(parents=True, exist_ok=True)
                    tmp=target.parent / f"{target.name}.tmp"
                    shutil.copy2(p,tmp); tmp.replace(target)
                    copied_total+=1

            # collect aggregate folder lists
            all_sources.add(str(src_folder))
            all_dests.add(str(dst_folder))

            key=f"{DOMAIN or 'NONE'}:{group_name}"
            group_report={
                "domain":DOMAIN,"source":str(src_folder),"dest":str(dst_folder),"glob":spec.source.glob,
                "candidates":len(candidates),"within_window":len(data_times_and_files),"selected":len(selected),
                "selected_after_gate":len(gated),"copied":0 if args.dry_run else len(gated),
                "operations":ops,
            }
            if gate_debug_entries:
                group_report["gate_debug"] = gate_debug_entries[:300]
            report["groups"][key]=group_report

    report["copied_total"]=0 if args.dry_run else copied_total
    report["folders"]["sources"] = sorted(all_sources)
    report["folders"]["destinations"] = sorted(all_dests)

    print(json.dumps(report,ensure_ascii=False,indent=2))
    return 0

if __name__=="__main__":
    raise SystemExit(main())

