#!/usr/bin/env python3
"""从跳板机并行采集超节点推理资源数据。仅依赖 Python 3 标准库。"""

import argparse
import base64
import contextlib
import csv
import html
import json
import math
import os
import queue
import re
import signal
import shlex
import socket
import statistics
import subprocess
import sys
import threading
import time
import unicodedata
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path


FIELDS = [
    "timestamp", "elapsed_s", "node", "role", "phase",
    "cpu_util_pct", "cpu_user_pct", "cpu_system_pct", "cpu_iowait_pct", "cpu_freq_avg_mhz", "cpu_freq_max_mhz", "cpu_temp_avg_c", "cpu_temp_max_c", "cpu_power_w",
    "load_1m", "load_5m", "load_15m", "host_mem_used_mib", "host_mem_available_mib",
    "host_mem_cache_mib", "host_mem_util_pct", "swap_used_mib", "swap_util_pct",
    "node_power_w", "dcu_index", "dcu_mem_used_mib", "dcu_mem_total_mib",
    "dcu_mem_util_pct", "dcu_util_pct", "dcu_power_w", "dcu_temp_c",
    "dcu_core_clock_mhz", "dcu_mem_clock_mhz", "error",
]

IB_FIELDS = [
    "timestamp", "elapsed_s", "node", "role", "phase", "hca", "port",
    "netdev", "state", "physical_state", "link_rate", "lid", "tx_gbps", "rx_gbps",
    "tx_link_util_pct", "rx_link_util_pct", "tx_packets_s", "rx_packets_s", "tx_bytes_total", "rx_bytes_total",
    "port_rcv_errors", "port_xmit_discards", "symbol_error", "link_downed",
    "local_link_integrity_errors", "excessive_buffer_overrun_errors", "vl15_dropped",
    "port_rcv_errors_delta", "port_xmit_discards_delta", "symbol_error_delta", "link_downed_delta",
    "local_link_integrity_errors_delta", "excessive_buffer_overrun_errors_delta", "vl15_dropped_delta",
]

NODE_METRICS = ["cpu_util_pct","cpu_user_pct","cpu_system_pct","cpu_iowait_pct",
    "cpu_freq_avg_mhz","cpu_freq_max_mhz","cpu_temp_avg_c","cpu_temp_max_c",
    "load_1m","load_5m","load_15m","host_mem_used_mib","host_mem_available_mib",
    "host_mem_cache_mib","host_mem_util_pct","swap_used_mib","swap_util_pct","cpu_power_w","node_power_w"]
DCU_METRICS = ["util_pct","mem_used_mib","mem_total_mib","mem_util_pct","power_w",
    "temp_c","temp_edge_c","temp_junction_c","temp_mem_c","temp_core_c","core_clock_mhz","mem_clock_mhz"]
COMPLETE_FIELDS = ["timestamp","elapsed_s","node","role","phase",*NODE_METRICS,
    "dcu_count","dcu_util_avg_pct","dcu_util_max_pct","dcu_mem_used_total_mib",
    "dcu_mem_total_mib","dcu_mem_util_pct","dcu_power_total_w"] + [f"dcu{i}_{m}" for i in range(4) for m in DCU_METRICS] + [
    "ib_port_count","ib_tx_total_gbps","ib_rx_total_gbps","ib_tx_packets_total_s",
    "ib_rx_packets_total_s","ib_tx_link_util_max_pct","ib_rx_link_util_max_pct",
    "ib_error_delta_total","ib_ports_json","error"]

# 面向日常查看的必需指标窄表。字段名直接携带单位，不受 metrics 输出开关裁剪。
CORE_FIELDS = [
    "timestamp", "elapsed_s", "node", "role", "phase",
    "cpu_util_pct", "cpu_power_w", "host_mem_used_gib", "host_mem_util_pct",
    "dcu_util_avg_pct", "dcu_mem_used_gib", "dcu_mem_total_gib", "dcu_mem_util_pct",
    "dcu_power_total_w", "node_power_w",
]

DEFAULT_METRICS = {k:True for k in ["cpu","cpu_power","load","host_memory","swap","node_power",
    "dcu_utilization","dcu_memory","dcu_power","dcu_temperature","dcu_clock",
    "ib_throughput","ib_packets","ib_link","ib_errors"]}


def metric_group(field):
    if field=="cpu_power_w":return "cpu_power"
    if field.startswith("cpu_"):return "cpu"
    if field.startswith("load_"):return "load"
    if field.startswith("host_mem_"):return "host_memory"
    if field.startswith("swap_"):return "swap"
    if field=="node_power_w":return "node_power"
    if field.startswith("ib_") or field in IB_FIELDS:
        if "error" in field or "discard" in field or "dropped" in field or field in ("symbol_error","link_downed","local_link_integrity_errors","excessive_buffer_overrun_errors"):return "ib_errors"
        if "packet" in field:return "ib_packets"
        if "gbps" in field:return "ib_throughput"
        return "ib_link"
    if field.startswith("dcu"):
        if "mem_" in field:return "dcu_memory"
        if "power" in field:return "dcu_power"
        if "temp" in field:return "dcu_temperature"
        if "clock" in field:return "dcu_clock"
        return "dcu_utilization"
    return None


def selected_fields(all_fields, metrics):
    always={"timestamp","elapsed_s","node","role","phase","error"}
    any_dcu=any(metrics.get(k,False) for k in metrics if k.startswith("dcu_"))
    any_ib=any(metrics.get(k,False) for k in metrics if k.startswith("ib_"))
    result=[]
    for field in all_fields:
        if field in always or (field in ("dcu_index","dcu_count") and any_dcu) or (field in ("hca","port","ib_port_count") and any_ib):result.append(field); continue
        group=metric_group(field)
        if group and metrics.get(group,False):result.append(field)
    return result

NODE_AGENT = r'''
import json, os, re, shlex, subprocess, sys, time

interval=float(sys.argv[1]); dcu_cmd=sys.argv[2]; power_cmd=sys.argv[3]

def run(cmd, timeout=5):
    if not cmd: return ""
    try:
        return subprocess.run(cmd, shell=True, universal_newlines=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=timeout).stdout.strip()
    except Exception as e: return "ERROR: "+repr(e)

def cpu_ticks():
    p=list(map(int, open('/proc/stat').readline().split()[1:])); p += [0]*(8-len(p))
    return p[:8]

def memory():
    m={}
    for line in open('/proc/meminfo'):
        k,v=line.split(':',1); m[k]=float(v.strip().split()[0])/1024
    total=m.get('MemTotal',0); avail=m.get('MemAvailable',m.get('MemFree',0))
    used=max(0,total-avail); cache=m.get('Cached',0)+m.get('SReclaimable',0)
    swap_total=m.get('SwapTotal',0); swap_used=max(0,swap_total-m.get('SwapFree',0))
    return {'host_mem_used_mib':used,'host_mem_total_mib':total,'host_mem_available_mib':avail,
      'host_mem_cache_mib':cache,'host_mem_util_pct':(100*used/total if total else None),
      'swap_used_mib':swap_used,'swap_util_pct':(100*swap_used/swap_total if swap_total else 0)}

def cpu_percent(old,new):
    d=[max(0,b-a) for a,b in zip(old,new)]; total=sum(d)
    if not total:return {}
    return {'cpu_util_pct':100*(1-(d[3]+d[4])/total),'cpu_user_pct':100*(d[0]+d[1])/total,
      'cpu_system_pct':100*(d[2]+d[5]+d[6])/total,'cpu_iowait_pct':100*d[4]/total}

def read_text(path, default=''):
    try:return open(path).read().strip()
    except Exception:return default

def read_int(path):
    try:return int(read_text(path,'0'))
    except Exception:return 0

IB_COUNTERS=['port_xmit_data','port_rcv_data','port_xmit_packets','port_rcv_packets',
 'port_rcv_errors','port_xmit_discards','symbol_error','link_downed',
 'local_link_integrity_errors','excessive_buffer_overrun_errors','VL15_dropped']

def ib_snapshot():
    result={}
    root='/sys/class/infiniband'
    if not os.path.isdir(root):return result
    for hca in os.listdir(root):
      netroot=os.path.join(root,hca,'device','net'); netdev=','.join(sorted(os.listdir(netroot))) if os.path.isdir(netroot) else ''
      ports=os.path.join(root,hca,'ports')
      if not os.path.isdir(ports):continue
      for port in os.listdir(ports):
        base=os.path.join(ports,port); counters=os.path.join(base,'counters')
        x={k:read_int(os.path.join(counters,k)) for k in IB_COUNTERS}
        x.update({'hca':hca,'port':port,'netdev':netdev,'state':read_text(os.path.join(base,'state')),
          'physical_state':read_text(os.path.join(base,'phys_state')),'link_rate':read_text(os.path.join(base,'rate')),
          'lid':read_text(os.path.join(base,'lid'))})
        result[(hca,port)]=x
    return result

def ib_rates(old,new,dt):
    rows=[]
    for key,x in new.items():
      r=dict(x); prev=old.get(key,{})
      xd=max(0,x['port_xmit_data']-prev.get('port_xmit_data',x['port_xmit_data']))
      rd=max(0,x['port_rcv_data']-prev.get('port_rcv_data',x['port_rcv_data']))
      # InfiniBand port_*_data 的计数单位是 4 字节（32-bit words）。
      r['tx_gbps']=xd*32/dt/1e9 if dt else None; r['rx_gbps']=rd*32/dt/1e9 if dt else None
      lm=re.search(r'(\d+(?:\.\d+)?)\s*Gb',x.get('link_rate',''),re.I); capacity=float(lm.group(1)) if lm else None
      r['tx_link_util_pct']=100*r['tx_gbps']/capacity if capacity else None
      r['rx_link_util_pct']=100*r['rx_gbps']/capacity if capacity else None
      r['tx_packets_s']=max(0,x['port_xmit_packets']-prev.get('port_xmit_packets',x['port_xmit_packets']))/dt if dt else None
      r['rx_packets_s']=max(0,x['port_rcv_packets']-prev.get('port_rcv_packets',x['port_rcv_packets']))/dt if dt else None
      r['tx_bytes_total']=x['port_xmit_data']*4; r['rx_bytes_total']=x['port_rcv_data']*4
      for rawkey in IB_COUNTERS[:4]: r.pop(rawkey,None)
      for counter in IB_COUNTERS[4:]:
        outkey='vl15_dropped' if counter=='VL15_dropped' else counter
        r[outkey+'_delta']=max(0,x[counter]-prev.get(counter,x[counter]))
      r['vl15_dropped']=r.pop('VL15_dropped',0); rows.append(r)
    return rows

def number(v):
    if isinstance(v,(int,float)): return float(v)
    if v is None: return None
    z=re.search(r'-?\d+(?:\.\d+)?',str(v).replace(',',''))
    return float(z.group()) if z else None

def mib(v, key=''):
    n=number(v)
    if n is None: return None
    s=(str(v)+' '+key).lower()
    if 'gib' in s or re.search(r'\bgb\b',s): n*=1024
    elif 'kib' in s or re.search(r'\bkb\b',s): n/=1024
    elif ('byte' in s or re.search(r'\bb\b',s)) and 'mb' not in s: n/=1048576
    return n

def find_value(obj, include, exclude=()):
    found=[]
    def walk(x):
        if isinstance(x,dict):
            for k,v in x.items():
                lk=str(k).lower()
                if all(t in lk for t in include) and not any(t in lk for t in exclude) and not isinstance(v,(dict,list)):
                    found.append((k,v))
                walk(v)
        elif isinstance(x,list):
            for v in x: walk(v)
    walk(obj); return found[0] if found else ('',None)

def json_cards(text):
    try: root=json.loads(text)
    except Exception: return []
    if isinstance(root,dict):
        items=[]
        for k,v in root.items():
            if isinstance(v,dict) and re.search(r'(card|gpu|dcu|device).*\d|^\d+$',str(k),re.I): items.append((k,v))
        if not items: items=[('0',root)]
    elif isinstance(root,list): items=[(str(i),v) for i,v in enumerate(root)]
    else: return []
    cards=[]
    for pos,(name,x) in enumerate(items):
        idx=number(name); idx=int(idx) if idx is not None else pos
        _,util=find_value(x,('use',),('memory','mem'));
        if util is None: _,util=find_value(x,('util',),('memory','mem'))
        pk,power=find_value(x,('power',),('cap','max'))
        mk,mem_used=find_value(x,('memory','used'))
        if mem_used is None: mk,mem_used=find_value(x,('vram','used'))
        tk,mem_total=find_value(x,('memory','total'),('used',))
        if mem_total is None: tk,mem_total=find_value(x,('vram','total'),('used',))
        _,mem_pct=find_value(x,('memory','use'))
        if mem_pct is None: _,mem_pct=find_value(x,('memory','util'))
        _,temp=find_value(x,('temp',))
        _,core_clock=find_value(x,('sclk',))
        if core_clock is None: _,core_clock=find_value(x,('core','clock'))
        _,mem_clock=find_value(x,('mclk',))
        if mem_clock is None: _,mem_clock=find_value(x,('memory','clock'))
        mem_used_norm=mib(mem_used,mk); mem_total_norm=mib(mem_total,tk); mem_pct_norm=number(mem_pct)
        if (mem_pct_norm is None or mem_pct_norm>100) and mem_total_norm: mem_pct_norm=100*mem_used_norm/mem_total_norm
        cards.append({'index':idx,'util_pct':number(util),'power_w':number(power),
            'mem_used_mib':mem_used_norm,'mem_total_mib':mem_total_norm,'mem_util_pct':mem_pct_norm,
            'temp_c':number(temp),'core_clock_mhz':number(core_clock),'mem_clock_mhz':number(mem_clock)})
    return cards

def text_cards(text):
    # 可解析常见 rocm-smi/hy-smi 文本；推荐配置带 --json 的命令。
    cards={}
    for line in text.splitlines():
        im=re.search(r'(?:card|gpu|dcu|device)\s*\[?\s*(\d+)\s*\]?',line,re.I)
        if not im: continue
        i=int(im.group(1)); c=cards.setdefault(i,{'index':i})
        patterns=[
          ('util_pct',r'(?:gpu|dcu)?\s*(?:use|util(?:ization)?)\D+(\d+(?:\.\d+)?)\s*%'),
          ('power_w',r'(?:average\s+)?power\D+(\d+(?:\.\d+)?)\s*w'),
          ('mem_used_mib',r'(?:vram|memory|mem)[^\n]*?used\D+(\d+(?:\.\d+)?)\s*(mib|mb|gib|gb)?'),
          ('mem_total_mib',r'(?:vram|memory|mem)[^\n]*?total\D+(\d+(?:\.\d+)?)\s*(mib|mb|gib|gb)?'),
        ]
        for key,pat in patterns:
            m=re.search(pat,line,re.I)
            if m:
                val=float(m.group(1)); unit=(m.group(2).lower() if m.lastindex and m.lastindex>1 and m.group(2) else '')
                c[key]=val*1024 if unit in ('gib','gb') else val
    for c in cards.values():
        u=c.get('mem_used_mib'); t=c.get('mem_total_mib')
        c['mem_util_pct']=100*u/t if u is not None and t else None
    return list(cards.values())

def node_power(text):
    # ipmitool dcmi power reading: Instantaneous power reading: 123 Watts
    for pat in [r'Instantaneous\s+power[^:]*:\s*(\d+(?:\.\d+)?)\s*W',r'(?i)\bpower\D+(\d+(?:\.\d+)?)\s*(?:W|Watts)']:
        m=re.search(pat,text,re.I)
        if m:return float(m.group(1))
    try:
        o=json.loads(text); _,v=find_value(o,('power',)); return number(v)
    except Exception:return number(text) if re.fullmatch(r'\s*\d+(?:\.\d+)?\s*',text) else None

last=cpu_ticks(); last_ib=ib_snapshot(); last_time=time.time(); time.sleep(min(interval,0.2))
while True:
    started=time.time(); now=cpu_ticks(); cpu=cpu_percent(last,now); last=now
    now_ib=ib_snapshot(); ibdt=max(.001,started-last_time); ib=ib_rates(last_ib,now_ib,ibdt); last_ib=now_ib; last_time=started
    loads=os.getloadavg(); raw=run(dcu_cmd); cards=json_cards(raw) or text_cards(raw)
    out={'ts':time.time(),**cpu,**memory(),'load_1m':loads[0],'load_5m':loads[1],'load_15m':loads[2],
         'node_power_w':node_power(run(power_cmd)),'dcus':cards,'ib':ib}
    if not cards: out['error']='DCU output parse failed: '+raw[:300].replace('\n',' | ')
    print(json.dumps(out,separators=(',',':')),flush=True)
    time.sleep(max(0,interval-(time.time()-started)))
'''

BASH_NODE_AGENT = r'''
interval="$1"; dcu_cmd="$2"; dcu_mem_cmd="$3"; power_cmd="$4"; cpu_power_cmd="$5"
dcu_mem_interval="$6"; node_power_interval="$7"; cpu_power_interval="$8"
cpu_temp_cmd="$9"; cpu_temp_interval="${10}"
dcu_temp_cmd="${11}"; dcu_temp_interval="${12}"
b64() { base64 | tr -d '\n'; }
read_counter() { [ -r "$1" ] && tr -d ' \n' < "$1" || printf '0'; }
auto_cpu_power() {
  files=$(ls /sys/class/hwmon/hwmon*/power*_input 2>/dev/null)
  [ -n "$files" ] || files=$(ls /sys/class/hwmon/hwmon*/power*_average 2>/dev/null)
  total=0; found=0
  for f in $files; do
    base=${f%_input}; base=${base%_average}; label=$(cat "${base}_label" 2>/dev/null)
    hwmon=${f%/*}; driver=$(cat "$hwmon/name" 2>/dev/null)
    desc=$(printf '%s %s' "$driver" "$label" | tr '[:upper:]' '[:lower:]')
    case "$desc" in *cpu*|*package*|*socket*|*ppt*|*k10temp*)
      value=$(cat "$f" 2>/dev/null)
      case "$value" in ''|*[!0-9.]*) continue;; esac
      total=$(awk -v a="$total" -v b="$value" 'BEGIN {printf "%.6f",a+b/1000000}'); found=1;;
    esac
  done
  [ "$found" -eq 1 ] && printf '%s' "$total"
}
auto_cpu_temperature() {
  total=0; maximum=0; found=0
  for f in /sys/class/hwmon/hwmon*/temp*_input; do
    [ -r "$f" ] || continue
    base=${f%_input}; label=$(cat "${base}_label" 2>/dev/null)
    hwmon=${f%/*}; driver=$(cat "$hwmon/name" 2>/dev/null)
    desc=$(printf '%s %s' "$driver" "$label" | tr '[:upper:]' '[:lower:]')
    case "$desc" in *cpu*|*package*|*socket*|*tctl*|*tdie*|*k10temp*)
      value=$(cat "$f" 2>/dev/null)
      case "$value" in ''|*[!0-9.-]*) continue;; esac
      value=$(awk -v v="$value" 'BEGIN {if(v>1000 || v< -1000)v=v/1000; printf "%.6f",v}')
      total=$(awk -v a="$total" -v b="$value" 'BEGIN {printf "%.6f",a+b}')
      maximum=$(awk -v a="$maximum" -v b="$value" 'BEGIN {if (b>a) print b; else print a}')
      found=$((found+1));;
    esac
  done
  [ "$found" -gt 0 ] && awk -v total="$total" -v count="$found" -v maximum="$maximum" 'BEGIN {printf "%.3f %.3f",total/count,maximum}'
}
last_dcu_mem=0; last_node_power=0; last_cpu_power=0; last_cpu_temp=0; last_dcu_temp=0
dcu_mem_raw=''; power_raw=''; cpu_power_raw=''; cpu_temp_raw=''; dcu_temp_raw=''
while :; do
  now_s=$(date +%s)
  ts=$(date +%s.%N)
  printf '@BEGIN\t%s\n' "$ts"
  awk '/^cpu / {printf "@CPU"; for(i=2;i<=9;i++) printf "\t%s",$i; printf "\n"; exit}' /proc/stat
  awk -F: '/^[[:space:]]*cpu MHz/ {v=$2+0; total+=v; count++; if(v>maximum)maximum=v} END {if(count)printf "@CPUFREQ\t%.3f\t%.3f\n",total/count,maximum}' /proc/cpuinfo
  awk '
    /^MemTotal:/ {mt=$2} /^MemAvailable:/ {ma=$2} /^MemFree:/ {mf=$2}
    /^Cached:/ {ca=$2} /^SReclaimable:/ {sr=$2}
    /^SwapTotal:/ {st=$2} /^SwapFree:/ {sf=$2}
    END {if(!ma)ma=mf; printf "@MEM\t%s\t%s\t%s\t%s\t%s\n",mt,ma,ca+sr,st,sf}' /proc/meminfo
  awk '{printf "@LOAD\t%s\t%s\t%s\n",$1,$2,$3}' /proc/loadavg
  dcu_raw=$(sh -c "$dcu_cmd" 2>&1); printf '@DCU\t'; printf '%s' "$dcu_raw" | b64; printf '\n'
  if [ -n "$dcu_mem_cmd" ] && [ $((now_s-last_dcu_mem)) -ge "$dcu_mem_interval" ]; then
    new_raw=$(sh -c "$dcu_mem_cmd" 2>&1); command_status=$?; last_dcu_mem=$now_s
    [ "$command_status" -eq 0 ] && dcu_mem_raw=$new_raw
  fi
  printf '@DCUMEM\t'; printf '%s' "$dcu_mem_raw" | b64; printf '\n'
  if [ -n "$dcu_temp_cmd" ] && [ $((now_s-last_dcu_temp)) -ge "$dcu_temp_interval" ]; then
    new_raw=$(sh -c "$dcu_temp_cmd" 2>&1); command_status=$?; last_dcu_temp=$now_s
    [ "$command_status" -eq 0 ] && dcu_temp_raw=$new_raw
  fi
  printf '@DCUTEMP\t'; printf '%s' "$dcu_temp_raw" | b64; printf '\n'
  if [ -n "$power_cmd" ] && [ $((now_s-last_node_power)) -ge "$node_power_interval" ]; then
    new_raw=$(sh -c "$power_cmd" 2>&1); command_status=$?; last_node_power=$now_s
    [ "$command_status" -eq 0 ] && power_raw=$new_raw
  fi
  printf '@POWER\t'; printf '%s' "$power_raw" | b64; printf '\n'
  if [ -n "$cpu_power_cmd" ] && [ $((now_s-last_cpu_power)) -ge "$cpu_power_interval" ]; then
    if [ "$cpu_power_cmd" = "auto" ]; then new_raw=$(auto_cpu_power); command_status=$?; else new_raw=$(sh -c "$cpu_power_cmd" 2>&1); command_status=$?; fi
    [ "$command_status" -eq 0 ] && cpu_power_raw=$new_raw
    last_cpu_power=$now_s
  fi
  printf '@CPUPOWER\t'; printf '%s' "$cpu_power_raw" | b64; printf '\n'
  if [ -n "$cpu_temp_cmd" ] && [ $((now_s-last_cpu_temp)) -ge "$cpu_temp_interval" ]; then
    if [ "$cpu_temp_cmd" = "auto" ]; then new_raw=$(auto_cpu_temperature); command_status=$?; else new_raw=$(sh -c "$cpu_temp_cmd" 2>&1); command_status=$?; fi
    [ "$command_status" -eq 0 ] && cpu_temp_raw=$new_raw
    last_cpu_temp=$now_s
  fi
  printf '@CPUTEMP\t'; printf '%s' "$cpu_temp_raw" | b64; printf '\n'
  for hpath in /sys/class/infiniband/*; do
    [ -d "$hpath" ] || continue; hca=${hpath##*/}
    netdev=$(ls "$hpath/device/net" 2>/dev/null | tr '\n' ',' | sed 's/,$//')
    for ppath in "$hpath"/ports/*; do
      [ -d "$ppath" ] || continue; port=${ppath##*/}; c="$ppath/counters"
      printf '@IB\t%s\t%s\t' "$hca" "$port"; printf '%s' "$netdev" | b64; printf '\t'
      read_counter "$ppath/state" | b64; printf '\t'; read_counter "$ppath/phys_state" | b64; printf '\t'
      read_counter "$ppath/rate" | b64; printf '\t'; read_counter "$ppath/lid"; printf '\t'
      for name in port_xmit_data port_rcv_data port_xmit_packets port_rcv_packets port_rcv_errors port_xmit_discards symbol_error link_downed local_link_integrity_errors excessive_buffer_overrun_errors VL15_dropped; do
        read_counter "$c/$name"; printf '\t'
      done
      printf '\n'
    done
  done
  printf '@END\n'
  sleep "$interval"
done
'''

DCU_UTIL_BASH_AGENT = r'''
interval="$1"; util_cmd="$2"
while :; do
  started=$(date +%s); ts=$(date +%s.%N)
  raw=$(sh -c "$util_cmd" 2>&1); status=$?
  if [ "$status" -eq 0 ]; then
    printf '%s\t' "$ts"; printf '%s' "$raw" | base64 | tr -d '\n'; printf '\n'
  fi
  ended=$(date +%s); spent=$((ended-started)); wait_s=$((interval-spent))
  [ "$wait_s" -gt 0 ] && sleep "$wait_s"
done
'''


def _num(v):
    if isinstance(v,(int,float)): return float(v)
    if v is None: return None
    z=re.search(r'-?\d+(?:\.\d+)?',str(v).replace(',',''))
    return float(z.group()) if z else None


def _mib(v,key=""):
    n=_num(v)
    if n is None:return None
    s=(str(v)+" "+key).lower()
    if "gib" in s or re.search(r'\bgb\b',s):n*=1024
    elif "kib" in s or re.search(r'\bkb\b',s):n/=1024
    elif ("byte" in s or re.search(r'\bb\b',s)) and "mb" not in s:n/=1048576
    return n


def _find(obj,include,exclude=()):
    found=[]
    def walk(x):
        if isinstance(x,dict):
            for k,v in x.items():
                lk=str(k).lower()
                if all(t in lk for t in include) and not any(t in lk for t in exclude) and not isinstance(v,(dict,list)):found.append((k,v))
                walk(v)
        elif isinstance(x,list):
            for v in x:walk(v)
    walk(obj); return found[0] if found else ('',None)


def parse_dcu_output(text):
    try:root=json.loads(text)
    except Exception:return []
    if isinstance(root,dict):
        items=[(k,v) for k,v in root.items() if isinstance(v,dict) and re.search(r'(card|gpu|dcu|device).*\d|^\d+$',str(k),re.I)]
        if not items:items=[('0',root)]
    elif isinstance(root,list):items=[(str(i),v) for i,v in enumerate(root)]
    else:return []
    cards=[]
    for pos,(name,x) in enumerate(items):
        idx=_num(name); idx=int(idx) if idx is not None else pos
        _,util=_find(x,('hcu','use'),('memory','mem'))
        if util is None:_,util=_find(x,('use',),('memory','mem'))
        if util is None:_,util=_find(x,('util',),('memory','mem'))
        _,power=_find(x,('average','graphics','package','power'),('cap','max'))
        if power is None:_,power=_find(x,('power',),('cap','max'))
        mk,mu=_find(x,('memory','used'))
        if mu is None:mk,mu=_find(x,('vram','used'))
        tk,mt=_find(x,('memory','total'),('used',))
        if mt is None:tk,mt=_find(x,('vram','total'),('used',))
        _,mp=_find(x,('memory','use'))
        if mp is None:_,mp=_find(x,('memory','util'))
        _,temp_edge=_find(x,('temp','edge')); _,temp_junction=_find(x,('temp','junction'))
        _,temp_mem=_find(x,('temp','mem')); _,temp_core=_find(x,('temp','core'))
        _,temp_any=_find(x,('temp',)); _,cc=_find(x,('sclk',)); _,mc=_find(x,('mclk',))
        mu_norm=_mib(mu,mk); mt_norm=_mib(mt,tk); mp_norm=_num(mp)
        # hy-smi 的 "Total Used Memory" 同时包含 memory/use 关键词，不能把很小的
        # 已用 MiB（例如 2 MiB）误当成 2%。容量齐全时始终以容量计算占用率。
        if mu_norm is not None and mt_norm:mp_norm=100*mu_norm/mt_norm
        cards.append({'index':idx,'util_pct':_num(util),'power_w':_num(power),'mem_used_mib':mu_norm,
          'mem_total_mib':mt_norm,'mem_util_pct':mp_norm,
          'temp_c':(_num(temp_junction) if _num(temp_junction) is not None else _num(temp_edge) if _num(temp_edge) is not None else _num(temp_any)),
          'temp_edge_c':_num(temp_edge),'temp_junction_c':_num(temp_junction),
          'temp_mem_c':_num(temp_mem),'temp_core_c':_num(temp_core),
          'core_clock_mhz':_num(cc),'mem_clock_mhz':_num(mc)})
    return cards


def merge_dcu_cards(primary,supplemental):
    """按卡号合并额外 hy-smi 输出；补充命令的非空字段优先。"""
    merged={c.get("index"):dict(c) for c in primary}
    for card in supplemental:
        idx=card.get("index"); target=merged.setdefault(idx,{"index":idx})
        for key,value in card.items():
            if key!="index" and value is not None:target[key]=value
    return [merged[k] for k in sorted(merged,key=lambda x:(x is None,str(x)))]


def parse_hcu_active_output(text):
    """解析 hy-smi --showhcuutil 的最近1秒 HCU active ratio 文本输出。"""
    cards=[]
    pattern=r'HCU\[(\d+)\].*?HCU\s+util\s+in\s+last\s+second\s*:\s*(\d+(?:\.\d+)?)\s*%'
    for index,value in re.findall(pattern,text or "",re.I):
        cards.append({'index':int(index),'util_pct':float(value)})
    return cards


def parse_power_output(text):
    for pat in [r'Instantaneous\s+power[^:]*:\s*(\d+(?:\.\d+)?)\s*W',
                r'Sensor\s+Reading\s*:\s*(\d+(?:\.\d+)?).*?Watts',
                r'\bpower\D+(\d+(?:\.\d+)?)\s*(?:W|Watts)']:
        m=re.search(pat,text,re.I)
        if m:return float(m.group(1))
    return _num(text) if re.match(r'^\s*\d+(?:\.\d+)?\s*$',text) else None


def parse_temperature_output(text):
    values=[float(x) for x in re.findall(r'-?\d+(?:\.\d+)?',text or "")]
    values=[v/1000 if abs(v)>1000 else v for v in values]
    values=[v for v in values if -20<=v<=150]
    if not values:return None,None
    # auto 命令输出“平均值 最大值”；厂商命令输出多个温度时则重新计算。
    if len(values)==2 and values[1]>=values[0]:return values[0],values[1]
    return sum(values)/len(values),max(values)


def mean(xs):
    vals=[float(x) for x in xs if x not in (None,"")]
    return sum(vals)/len(vals) if vals else None


def cv(xs):
    vals=[float(x) for x in xs if x is not None]
    avg=sum(vals)/len(vals) if vals else 0
    if len(vals)<2 or abs(avg)<1e-9: return math.inf
    return statistics.pstdev(vals)/abs(avg)


def strip_json_comments(text):
    """删除 JSONC 的 // 和 /* ... */ 注释，同时保留字符串中的 //。"""
    out=[]; i=0; in_string=False; escaped=False
    while i<len(text):
        ch=text[i]; nxt=text[i+1] if i+1<len(text) else ""
        if in_string:
            out.append(ch)
            if escaped:escaped=False
            elif ch=="\\":escaped=True
            elif ch=='"':in_string=False
            i+=1; continue
        if ch=='"':in_string=True; out.append(ch); i+=1; continue
        if ch=='/' and nxt=='/':
            i+=2
            while i<len(text) and text[i] not in "\r\n":i+=1
            continue
        if ch=='/' and nxt=='*':
            i+=2
            while i+1<len(text) and not (text[i]=='*' and text[i+1]=='/'):
                if text[i] in "\r\n":out.append(text[i])
                i+=1
            i+=2; continue
        out.append(ch); i+=1
    return "".join(out)


def load_config(path, nodes_override=None):
    cfg=json.loads(strip_json_comments(Path(path).read_text(encoding="utf-8")))
    cfg["metrics"]={**DEFAULT_METRICS,**cfg.get("metrics",{})}
    try:
        base=int(cfg.get("sample_interval_s",2))
        if base<=0:raise ValueError
        cfg["sample_interval_s"]=base
        for key,default in (("dcu_memory_interval_s",10),("dcu_utilization_interval_s",5),("dcu_temperature_interval_s",5),("node_power_interval_s",5),("cpu_power_interval_s",5),("cpu_temperature_interval_s",5)):
            value=int(cfg.get(key,default))
            if value<=0:raise ValueError
            # 远端循环只能在基础样本时刻执行命令，小于基础周期没有实际意义。
            cfg[key]=max(base,value)
    except (TypeError,ValueError):
        raise SystemExit("采样周期必须是大于 0 的整数秒")
    defaults={"reference_group":"auto","window_fresh_samples":6,"confirm_windows":2,
              "active_threshold_pct":5.0,"max_half_mean_change_pct":10.0,
              "idle_threshold_pct":2.0,"idle_confirm_samples":2}
    steady={**defaults,**cfg.get("steady_state",{})}
    steady.pop("enabled",None)  # 兼容旧配置：该开关已移除，始终计算两种稳态口径。
    try:
        steady["reference_group"]=str(steady["reference_group"]).strip().upper() or "AUTO"
        for key in ("window_fresh_samples","confirm_windows","idle_confirm_samples"):
            steady[key]=int(steady[key])
            if steady[key]<2:raise ValueError
        for key in ("active_threshold_pct","max_half_mean_change_pct","idle_threshold_pct"):
            steady[key]=float(steady[key])
            if steady[key]<0:raise ValueError
        if steady["window_fresh_samples"]<4 or steady["idle_threshold_pct"]>=steady["active_threshold_pct"]:raise ValueError
    except (TypeError,ValueError):
        raise SystemExit("steady_state 参数无效：窗口至少4个样本，确认次数至少2，且空闲阈值必须小于活动阈值")
    cfg["steady_state"]=steady
    if nodes_override:
        cfg["deployment"]={"mode":"CUSTOM","groups":{"CUSTOM":[x.strip() for x in nodes_override.split(",") if x.strip()]}}
    return cfg


def spawn_node(node, cfg, outq, stop):
    # 计算节点无需 Python：远端 Bash 只读取 proc/sysfs 和运行监控命令，解析全部在跳板机完成。
    remote=" ".join(["bash","-c",shlex.quote(BASH_NODE_AGENT),"monitor",
        shlex.quote(str(cfg["sample_interval_s"])),shlex.quote(cfg["dcu_command"]),
        shlex.quote(cfg.get("dcu_memory_command","")),shlex.quote(cfg.get("node_power_command","")),
        shlex.quote(cfg.get("cpu_power_command","auto")),
        shlex.quote(str(cfg["dcu_memory_interval_s"])),shlex.quote(str(cfg["node_power_interval_s"])),
        shlex.quote(str(cfg["cpu_power_interval_s"])),shlex.quote(cfg.get("cpu_temperature_command","auto")),
        shlex.quote(str(cfg["cpu_temperature_interval_s"])),shlex.quote(cfg.get("dcu_temperature_command","")),
        shlex.quote(str(cfg["dcu_temperature_interval_s"]))])
    ssh=["ssh", *cfg.get("ssh_options",[]), node, remote]
    try:
        proc=subprocess.Popen(ssh, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                              bufsize=1, encoding="utf-8", errors="replace")
    except Exception as e:
        outq.put((node,None,f"SSH start failed: {e}")); return
    def stderr_reader():
        for line in proc.stderr or []:
            msg=line.strip()
            if msg and not (stop.is_set() and "Killed by signal" in msg):
                outq.put((node,None,"SSH stderr: "+msg))
    threading.Thread(target=stderr_reader,daemon=True).start()
    def decode64(value):
        try:return base64.b64decode(value).decode("utf-8","replace")
        except Exception:return ""
    current={}; last_cpu=None; last_ib={}; last_ts=None
    counters=['port_xmit_data','port_rcv_data','port_xmit_packets','port_rcv_packets','port_rcv_errors',
              'port_xmit_discards','symbol_error','link_downed','local_link_integrity_errors',
              'excessive_buffer_overrun_errors','VL15_dropped']
    try:
        for line in proc.stdout or []:
            if stop.is_set(): break
            parts=line.rstrip("\r\n").split("\t"); tag=parts[0]
            try:
                if tag=="@BEGIN": current={'ts':float(parts[1]),'ib_raw':[]}
                elif tag=="@CPU": current['cpu_raw']=[int(x) for x in parts[1:9]]
                elif tag=="@CPUFREQ": current['cpu_freq_raw']=[float(x) for x in parts[1:3]]
                elif tag=="@MEM": current['mem_raw']=[float(x) for x in parts[1:6]]
                elif tag=="@LOAD": current['loads']=[float(x) for x in parts[1:4]]
                elif tag=="@DCU": current['dcu_raw']=decode64(parts[1])
                elif tag=="@DCUMEM": current['dcu_mem_raw']=decode64(parts[1])
                elif tag=="@DCUTEMP": current['dcu_temp_raw']=decode64(parts[1])
                elif tag=="@POWER": current['power_raw']=decode64(parts[1])
                elif tag=="@CPUPOWER": current['cpu_power_raw']=decode64(parts[1])
                elif tag=="@CPUTEMP": current['cpu_temp_raw']=decode64(parts[1])
                elif tag=="@IB":
                    vals=[int(x or 0) for x in parts[8:19]]
                    x={'hca':parts[1],'port':parts[2],'netdev':decode64(parts[3]),'state':decode64(parts[4]),
                       'physical_state':decode64(parts[5]),'link_rate':decode64(parts[6]),'lid':parts[7]}
                    x.update(dict(zip(counters,vals))); current['ib_raw'].append(x)
                elif tag=="@END":
                    sample={'ts':current.get('ts',time.time())}; now_cpu=current.get('cpu_raw')
                    if last_cpu and now_cpu:
                        d=[max(0,b-a) for a,b in zip(last_cpu,now_cpu)]; total=sum(d)
                        if total:
                            sample.update({'cpu_util_pct':100*(1-(d[3]+d[4])/total),
                              'cpu_user_pct':100*(d[0]+d[1])/total,'cpu_system_pct':100*(d[2]+d[5]+d[6])/total,
                              'cpu_iowait_pct':100*d[4]/total})
                    last_cpu=now_cpu or last_cpu
                    cpu_freq=current.get('cpu_freq_raw',[])
                    if len(cpu_freq)==2:sample.update({'cpu_freq_avg_mhz':cpu_freq[0],'cpu_freq_max_mhz':cpu_freq[1]})
                    mem=current.get('mem_raw',[])
                    if len(mem)==5:
                        total,avail,cache,swap_total,swap_free=[x/1024 for x in mem]; used=max(0,total-avail); swap_used=max(0,swap_total-swap_free)
                        sample.update({'host_mem_total_mib':total,'host_mem_available_mib':avail,'host_mem_used_mib':used,
                          'host_mem_cache_mib':cache,'host_mem_util_pct':100*used/total if total else None,
                          'swap_used_mib':swap_used,'swap_util_pct':100*swap_used/swap_total if swap_total else 0})
                    loads=current.get('loads',[])
                    if len(loads)==3:sample.update(dict(zip(['load_1m','load_5m','load_15m'],loads)))
                    primary_cards=parse_dcu_output(current.get('dcu_raw',''))
                    sample['dcus']=merge_dcu_cards(primary_cards,
                                                   parse_dcu_output(current.get('dcu_mem_raw','')))
                    sample['dcus']=merge_dcu_cards(sample['dcus'],parse_dcu_output(current.get('dcu_temp_raw','')))
                    sample['node_power_w']=parse_power_output(current.get('power_raw',''))
                    sample['cpu_power_w']=parse_power_output(current.get('cpu_power_raw',''))
                    sample['cpu_temp_avg_c'],sample['cpu_temp_max_c']=parse_temperature_output(current.get('cpu_temp_raw',''))
                    now_ts=sample['ts']; dt=max(.001,now_ts-last_ts) if last_ts is not None else None; ib=[]
                    for x in current.get('ib_raw',[]):
                        key=(x['hca'],x['port']); prev=last_ib.get(key,{}); r=dict(x)
                        xd=max(0,x['port_xmit_data']-prev.get('port_xmit_data',x['port_xmit_data']))
                        rd=max(0,x['port_rcv_data']-prev.get('port_rcv_data',x['port_rcv_data']))
                        r['tx_gbps']=xd*32/dt/1e9 if dt else None; r['rx_gbps']=rd*32/dt/1e9 if dt else None
                        lm=re.search(r'(\d+(?:\.\d+)?)\s*Gb',x.get('link_rate',''),re.I); capacity=float(lm.group(1)) if lm else None
                        r['tx_link_util_pct']=100*r['tx_gbps']/capacity if capacity and dt else None
                        r['rx_link_util_pct']=100*r['rx_gbps']/capacity if capacity and dt else None
                        r['tx_packets_s']=max(0,x['port_xmit_packets']-prev.get('port_xmit_packets',x['port_xmit_packets']))/dt if dt else None
                        r['rx_packets_s']=max(0,x['port_rcv_packets']-prev.get('port_rcv_packets',x['port_rcv_packets']))/dt if dt else None
                        r['tx_bytes_total']=x['port_xmit_data']*4; r['rx_bytes_total']=x['port_rcv_data']*4
                        for k in counters[4:]:r[('vl15_dropped' if k=='VL15_dropped' else k)+'_delta']=max(0,x[k]-prev.get(k,x[k]))
                        r['vl15_dropped']=r.pop('VL15_dropped',0)
                        for k in counters[:4]:r.pop(k,None)
                        ib.append(r); last_ib[key]=x
                    last_ts=now_ts; sample['ib']=ib
                    if not sample['dcus']:sample['error']='DCU output parse failed: '+current.get('dcu_raw','')[:300].replace('\n',' | ')
                    outq.put((node,sample,None)); current={}
            except Exception as e: outq.put((node,None,"Bash protocol parse failed: "+repr(e)+"; line="+line[:200]))
    finally:
        if proc.poll() is None: proc.terminate()


def spawn_dcu_util(node,cfg,outq,stop):
    """独立采集较慢的最近1秒 HCU active ratio，避免阻塞基础资源采样。"""
    command=cfg.get("dcu_utilization_command","")
    if not command:return
    remote=" ".join(["bash","-c",shlex.quote(DCU_UTIL_BASH_AGENT),"util-monitor",
                     shlex.quote(str(cfg["dcu_utilization_interval_s"])),shlex.quote(command)])
    ssh=["ssh",*cfg.get("ssh_options",[]),node,remote]
    try:
        proc=subprocess.Popen(ssh,stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True,
                              bufsize=1,encoding="utf-8",errors="replace")
    except Exception as exc:
        outq.put(("__dcuutil__:"+node,None,"DCU利用率SSH启动失败: %s"%exc)); return
    def stderr_reader():
        for line in proc.stderr or []:
            message=line.strip()
            if message and not (stop.is_set() and "Killed by signal" in message):
                outq.put(("__dcuutil__:"+node,None,"SSH stderr: "+message))
    threading.Thread(target=stderr_reader,daemon=True).start()
    try:
        for line in proc.stdout or []:
            if stop.is_set():break
            parts=line.rstrip("\r\n").split("\t",1)
            if len(parts)!=2:continue
            try:
                raw=base64.b64decode(parts[1]).decode("utf-8","replace")
                outq.put(("__dcuutil__:"+node,{"ts":float(parts[0]),"cards":parse_hcu_active_output(raw)},None))
            except Exception as exc:
                outq.put(("__dcuutil__:"+node,None,"DCU利用率解析失败: %s"%exc))
    finally:
        if proc.poll() is None:proc.terminate()


def rows_for_sample(sample, node, role, started, phase, err=""):
    base={"timestamp":datetime.fromtimestamp(sample.get("ts",time.time()),timezone.utc).astimezone().isoformat(timespec="milliseconds"),
          "elapsed_s":round(time.time()-started,3),"node":node,"role":role,"phase":phase,
          **{k:sample.get(k) for k in ["cpu_util_pct","cpu_user_pct","cpu_system_pct","cpu_iowait_pct","cpu_power_w",
             "load_1m","load_5m","load_15m","host_mem_used_mib","host_mem_available_mib",
             "host_mem_cache_mib","host_mem_util_pct","swap_used_mib","swap_util_pct"]},
          "node_power_w":sample.get("node_power_w"),
          "error":err or sample.get("error","")}
    cards=sample.get("dcus") or [{}]
    result=[]
    for c in cards:
        r=dict(base); r.update({"dcu_index":c.get("index"),"dcu_mem_used_mib":c.get("mem_used_mib"),
            "dcu_mem_total_mib":c.get("mem_total_mib"),"dcu_mem_util_pct":c.get("mem_util_pct"),
            "dcu_util_pct":c.get("util_pct"),"dcu_power_w":c.get("power_w"),"dcu_temp_c":c.get("temp_c"),
            "dcu_core_clock_mhz":c.get("core_clock_mhz"),"dcu_mem_clock_mhz":c.get("mem_clock_mhz")}); result.append(r)
    return result


def ib_rows_for_sample(sample, node, role, started, phase):
    base={"timestamp":datetime.fromtimestamp(sample.get("ts",time.time()),timezone.utc).astimezone().isoformat(timespec="milliseconds"),
          "elapsed_s":round(time.time()-started,3),"node":node,"role":role,"phase":phase}
    return [{**base,**port} for port in sample.get("ib",[])]


def complete_row(sample, node, role, started, phase):
    row={"timestamp":datetime.fromtimestamp(sample.get("ts",time.time()),timezone.utc).astimezone().isoformat(timespec="milliseconds"),
         "elapsed_s":round(time.time()-started,3),"node":node,"role":role,"phase":phase,
         **{k:sample.get(k) for k in NODE_METRICS},"error":sample.get("error","")}
    cards=sample.get("dcus",[]); row["dcu_count"]=len(cards)
    def sum_present(items,key):
        vals=[x.get(key) for x in items if x.get(key) is not None]
        return sum(vals) if vals else None
    row["dcu_util_avg_pct"]=mean([x.get("util_pct") for x in cards])
    utils=[x.get("util_pct") for x in cards if x.get("util_pct") is not None]
    row["dcu_util_max_pct"]=max(utils) if utils else None
    row["dcu_mem_used_total_mib"]=sum_present(cards,"mem_used_mib")
    row["dcu_mem_total_mib"]=sum_present(cards,"mem_total_mib")
    row["dcu_mem_util_pct"]=(100*row["dcu_mem_used_total_mib"]/row["dcu_mem_total_mib"]
                              if row["dcu_mem_used_total_mib"] is not None and row["dcu_mem_total_mib"] else None)
    row["dcu_power_total_w"]=sum_present(cards,"power_w")
    for c in cards:
        try: idx=int(c.get("index"))
        except (TypeError,ValueError): continue
        if 0<=idx<4:
            for m in DCU_METRICS: row[f"dcu{idx}_{m}"]=c.get(m)
    ports=sample.get("ib",[]); row["ib_port_count"]=len(ports)
    for target,source in [("ib_tx_total_gbps","tx_gbps"),("ib_rx_total_gbps","rx_gbps"),
                          ("ib_tx_packets_total_s","tx_packets_s"),("ib_rx_packets_total_s","rx_packets_s")]:
        row[target]=sum_present(ports,source)
    for target,source in [("ib_tx_link_util_max_pct","tx_link_util_pct"),("ib_rx_link_util_max_pct","rx_link_util_pct")]:
        vals=[x.get(source) for x in ports if x.get(source) is not None]; row[target]=max(vals) if vals else None
    error_keys=["port_rcv_errors_delta","port_xmit_discards_delta","symbol_error_delta","link_downed_delta",
                "local_link_integrity_errors_delta","excessive_buffer_overrun_errors_delta","vl15_dropped_delta"]
    row["ib_error_delta_total"]=sum(x.get(k) or 0 for x in ports for k in error_keys)
    row["ib_ports_json"]=json.dumps(ports,ensure_ascii=False,separators=(",",":"))
    return row


def core_row(complete):
    """从完整节点样本生成固定字段、带明确单位的核心资源窄表行。"""
    def mib_to_gib(value):
        return value/1024 if value is not None else None
    return {
        "timestamp":complete.get("timestamp"), "elapsed_s":complete.get("elapsed_s"),
        "node":complete.get("node"), "role":complete.get("role"), "phase":complete.get("phase"),
        "cpu_util_pct":complete.get("cpu_util_pct"), "cpu_power_w":complete.get("cpu_power_w"),
        "host_mem_used_gib":mib_to_gib(complete.get("host_mem_used_mib")),
        "host_mem_util_pct":complete.get("host_mem_util_pct"),
        "dcu_util_avg_pct":complete.get("dcu_util_avg_pct"),
        "dcu_mem_used_gib":mib_to_gib(complete.get("dcu_mem_used_total_mib")),
        "dcu_mem_total_gib":mib_to_gib(complete.get("dcu_mem_total_mib")),
        "dcu_mem_util_pct":complete.get("dcu_mem_util_pct"),
        "dcu_power_total_w":complete.get("dcu_power_total_w"),
        "node_power_w":complete.get("node_power_w"),
    }


def summarize(rows, path, enabled=None):
    stable=[r for r in rows if r["phase"]=="recording"]
    groups=defaultdict(list)
    for r in stable: groups[(r["node"],r["role"],r["dcu_index"])].append(r)
    metrics=["cpu_util_pct","cpu_user_pct","cpu_system_pct","cpu_iowait_pct","cpu_power_w","load_1m",
             "host_mem_used_mib","host_mem_util_pct","swap_used_mib","node_power_w",
             "dcu_mem_used_mib","dcu_mem_util_pct","dcu_util_pct","dcu_power_w","dcu_temp_c"]
    enabled=enabled or DEFAULT_METRICS; metrics=[m for m in metrics if enabled.get(metric_group(m),False)]
    with open(path,"w",newline="",encoding="utf-8-sig") as f:
        cols=["node","role","dcu_index","samples"]+[x+s for x in metrics for s in ("_avg","_min","_max","_p95")]
        w=csv.DictWriter(f,fieldnames=cols); w.writeheader()
        for key,rs in sorted(groups.items(),key=lambda x:tuple(str(y) for y in x[0])):
            o=dict(zip(["node","role","dcu_index"],key)); o["samples"]=len(rs)
            for m in metrics:
                vs=sorted(float(r[m]) for r in rs if r[m] not in (None,""))
                if vs:
                    o[m+"_avg"]=round(sum(vs)/len(vs),3); o[m+"_min"]=round(vs[0],3); o[m+"_max"]=round(vs[-1],3)
                    o[m+"_p95"]=round(vs[min(len(vs)-1,math.ceil(.95*len(vs))-1)],3)
            w.writerow(o)


def summarize_ib(rows, path, enabled=None):
    stable=[r for r in rows if r["phase"]=="recording"]
    groups=defaultdict(list)
    for r in stable: groups[(r["node"],r["role"],r["hca"],r["port"])].append(r)
    metrics=["tx_gbps","rx_gbps","tx_link_util_pct","rx_link_util_pct","tx_packets_s","rx_packets_s",
             "port_rcv_errors_delta","port_xmit_discards_delta","symbol_error_delta","link_downed_delta",
             "local_link_integrity_errors_delta","excessive_buffer_overrun_errors_delta","vl15_dropped_delta"]
    enabled=enabled or DEFAULT_METRICS; metrics=[m for m in metrics if enabled.get(metric_group(m),False)]
    with open(path,"w",newline="",encoding="utf-8-sig") as f:
        cols=["node","role","hca","port","samples"]+[x+s for x in metrics for s in ("_avg","_min","_max","_p95")]
        w=csv.DictWriter(f,fieldnames=cols); w.writeheader()
        for key,rs in sorted(groups.items(),key=lambda x:tuple(str(y) for y in x[0])):
            o=dict(zip(["node","role","hca","port"],key)); o["samples"]=len(rs)
            for m in metrics:
                vs=sorted(float(r[m]) for r in rs if r.get(m) not in (None,""))
                if vs:
                    o[m+"_avg"]=round(sum(vs)/len(vs),3); o[m+"_min"]=round(vs[0],3); o[m+"_max"]=round(vs[-1],3)
                    o[m+"_p95"]=round(vs[min(len(vs)-1,math.ceil(.95*len(vs))-1)],3)
            w.writerow(o)


def terminal_summary(complete_rows,metrics=None):
    metrics=metrics or DEFAULT_METRICS
    stable=[r for r in complete_rows if r["phase"]=="recording"]
    scope="稳定阶段"
    if not stable:
        stable=list(complete_rows); scope="未判稳采集区间（仅供排查，不是正式测试结果）"
    if not stable: return "没有收到任何节点采样，无法生成数值汇总。"
    by_node=defaultdict(list)
    for r in stable: by_node[r["node"]].append(r)
    def avg(rs,key): return mean([r.get(key) for r in rs])
    def p95(rs,key):
        vals=sorted(float(r[key]) for r in rs if r.get(key) not in (None,""))
        return vals[min(len(vals)-1,math.ceil(.95*len(vals))-1)] if vals else None
    def div(v,n):return v/n if v is not None else None
    node_stats={}
    for node,rs in by_node.items():
        node_stats[node]={"role":rs[0]["role"],"cpu":avg(rs,"cpu_util_pct"),
          "mem":div(avg(rs,"host_mem_used_mib"),1024),"dcu_util":avg(rs,"dcu_util_avg_pct"),
          "dcu_mem":div(avg(rs,"dcu_mem_used_total_mib"),1024),"dcu_power":avg(rs,"dcu_power_total_w"),
          "dcu_mem_total":div(avg(rs,"dcu_mem_total_mib"),1024),
          "cpu_power":avg(rs,"cpu_power_w"),"cpu_power_p95":p95(rs,"cpu_power_w"),
          "node_power":avg(rs,"node_power_w"),"node_power_p95":p95(rs,"node_power_w"),
          "ib_tx":avg(rs,"ib_tx_total_gbps"),"ib_rx":avg(rs,"ib_rx_total_gbps"),
          "ib_errors":sum(float(r.get("ib_error_delta_total") or 0) for r in rs)}
    def sum_known(values):
        vals=[x for x in values if x is not None]; return sum(vals) if vals else None
    def aggregate(label,nodes):
        ss=[node_stats[n] for n in nodes]; count=len(ss)
        dcu_mem_used=sum_known([x["dcu_mem"] for x in ss]); dcu_mem_total=sum_known([x["dcu_mem_total"] for x in ss])
        return {"范围":label,"节点数":count,"CPU均值(%)":mean([x["cpu"] for x in ss]),
          "主机内存(GiB)":sum_known([x["mem"] for x in ss]),"DCU利用率(%)":mean([x["dcu_util"] for x in ss]),
          "DCU显存(GiB)":dcu_mem_used,"DCU显存占用率(%)":(100*dcu_mem_used/dcu_mem_total if dcu_mem_used is not None and dcu_mem_total else None),
          "DCU功耗(W)":sum_known([x["dcu_power"] for x in ss]),
          "CPU功耗均值(W)":sum_known([x["cpu_power"] for x in ss]),"CPU功耗P95(W)":sum_known([x["cpu_power_p95"] for x in ss]),
          "整机功耗均值(W)":sum_known([x["node_power"] for x in ss]),"整机功耗P95(W)":sum_known([x["node_power_p95"] for x in ss]),
          "IB发送(Gbit/s)":sum_known([x["ib_tx"] for x in ss]),"IB接收(Gbit/s)":sum_known([x["ib_rx"] for x in ss]),
          "IB错误增量(次)":sum_known([x["ib_errors"] for x in ss])}
    all_nodes=sorted(node_stats); table=[aggregate("ALL",all_nodes)]
    role_order=[]
    for preferred in ("P","D","IFB","CUSTOM"):
        if any(node_stats[n]["role"].upper()==preferred for n in all_nodes):role_order.append(preferred)
    for role in role_order:
        ns=[n for n in all_nodes if node_stats[n]["role"].upper()==role]
        table.append(aggregate(role,ns))
        for n in ns:table.append(aggregate("  "+n,[n]))
    known=set(n for role in role_order for n in all_nodes if node_stats[n]["role"].upper()==role)
    for n in all_nodes:
        if n not in known:table.append(aggregate("  "+n,[n]))
    headers=["范围","节点数"]
    if metrics.get("cpu"):headers.append("CPU均值(%)")
    if metrics.get("cpu_power"):headers.append("CPU功耗均值(W)")
    if metrics.get("host_memory"):headers.append("主机内存(GiB)")
    if metrics.get("dcu_utilization"):headers.append("DCU利用率(%)")
    if metrics.get("dcu_memory"):headers.extend(["DCU显存(GiB)","DCU显存占用率(%)"])
    if metrics.get("dcu_power"):headers.append("DCU功耗(W)")
    if metrics.get("node_power"):headers.extend(["整机功耗均值(W)","整机功耗P95(W)"])
    if metrics.get("ib_throughput"):headers.extend(["IB发送(Gbit/s)","IB接收(Gbit/s)"])
    if metrics.get("ib_errors"):headers.append("IB错误增量(次)")
    formatted=[]
    for row in table:
        formatted.append([str(row[h]) if h in ("范围","节点数") else ("--" if row[h] is None else "%.2f"%row[h]) for h in headers])
    def display_width(value):
        return sum(0 if unicodedata.combining(ch) else (2 if unicodedata.east_asian_width(ch) in ("W","F") else 1) for ch in str(value))
    def pad(value,width):
        text=str(value); return text+" "*max(0,width-display_width(text))
    widths=[max(display_width(headers[i]),max(display_width(row[i]) for row in formatted)) for i in range(len(headers))]
    line=lambda row:" | ".join(pad(v,widths[i]) for i,v in enumerate(row))
    report="\n".join([scope+"资源汇总（分组行和 ALL 行为节点聚合值）",line(headers),"-+-".join("-"*w for w in widths),
                       *[line(row) for row in formatted]])
    if any("--" in row for row in formatted):
        report+="\n\n说明：-- 表示采集命令未返回该指标或当前解析器未识别对应字段，不代表数值为 0。"
    return report


def legacy_main():
    ap=argparse.ArgumentParser(description="超节点 PD/IFB 推理资源监控")
    ap.add_argument("--config",default="monitor_config.jsonc")
    ap.add_argument("--model",help="模型名称；覆盖 JSONC 中的 model_name，并用于结果目录/文件名前缀")
    ap.add_argument("--nodes",help="可选：直接指定节点，如 p1c0,p1c1；覆盖配置文件")
    ap.add_argument("--output-dir",default="results"); ap.add_argument("--no-wait-stable",action="store_true")
    args=ap.parse_args(); cfg=load_config(args.config,args.nodes)
    model_name=(args.model if args.model is not None else cfg.get("model_name","unnamed_model")).strip()
    if not model_name:model_name="unnamed_model"
    safe_model="".join(c if c.isalnum() or c in "-_." else "_" for c in model_name)[:80] or "unnamed_model"
    cfg["model_name"]=model_name
    deployment=cfg.get("deployment",{}); mode=str(deployment.get("mode","PD")).upper(); all_groups=deployment.get("groups",{})
    active_groups=({k:v for k,v in all_groups.items() if k.upper() in ("P","D")} if mode=="PD" else
                   {k:v for k,v in all_groups.items() if k.upper()==mode})
    roles={n:role.upper() for role,nodes in active_groups.items() for n in nodes}
    if sum(len(nodes) for nodes in active_groups.values())!=len(roles):
        raise SystemExit("节点配置重复：同一个计算节点不能同时属于多个部署组")
    if not roles: raise SystemExit("没有配置监控节点，请使用 --nodes p1c0,p1c1 或修改配置文件")
    metrics=cfg["metrics"]
    stamp=datetime.now().strftime("%Y%m%d_%H%M%S"); outdir=Path(args.output_dir)/(safe_model+"_run_"+stamp); outdir.mkdir(parents=True)
    prefix=safe_model+"_"
    complete_path=outdir/(prefix+"complete_samples.csv"); core_path=outdir/(prefix+"core_metrics.csv")
    raw_path=outdir/(prefix+"dcu_samples.csv"); summary_path=outdir/(prefix+"dcu_summary.csv")
    ib_path=outdir/(prefix+"ib_samples.csv"); ib_summary_path=outdir/(prefix+"ib_summary.csv"); log_path=outdir/(prefix+"summary.log")
    (outdir/(prefix+"effective_config.json")).write_text(json.dumps(cfg,ensure_ascii=False,indent=2),encoding="utf-8")
    q=queue.Queue(); stop=threading.Event(); threads=[]; rows=[]; ib_rows=[]; complete_rows=[]; started=time.time()
    for node in roles:
        t=threading.Thread(target=spawn_node,args=(node,cfg,q,stop),daemon=True); t.start(); threads.append(t)
    stability=cfg["stability"]; samples_per_node=max(2,int(math.ceil(stability["window_s"]/cfg["sample_interval_s"])))
    histories={n:deque(maxlen=samples_per_node) for n in roles}; stable_checks=0; recording_since=None; last_stability_check=0
    signal.signal(signal.SIGINT,lambda *_:stop.set())
    seen=set(); latest={}; last_status=started
    def series_stable(vals,max_cv):
        vals=[v for v in vals if v is not None]
        if len(vals)<samples_per_node:return False
        avg=mean(vals)
        if avg<=stability["idle_util_threshold_pct"]:
            return max(vals)-min(vals)<=stability["max_idle_util_range_pct"]
        return cv(vals)<=max_cv
    def stability_snapshot():
        full_nodes=[n for n,xs in histories.items() if len(xs)>=samples_per_node]
        all_utils=[x[1] for xs in histories.values() for x in xs if x[1] is not None]; cluster_util=mean(all_utils)
        util_pass=[]; util_fail=[]
        for n,xs in histories.items():
            (util_pass if series_stable([x[1] for x in xs],stability["max_util_cv"]) else util_fail).append(n)
        return {"full_nodes":full_nodes,"cluster_util":cluster_util,"util_pass":util_pass,"util_fail":util_fail}
    def show_status(force=False):
        nonlocal last_status
        now=time.time()
        if not force and now-last_status<10:return
        elapsed=now-started; snap=stability_snapshot(); avg_util=snap["cluster_util"]
        failures=sum(1 for s in latest.values() if not s.get("dcus"))
        if recording_since: stage="正式记录中 %.0f/%ss"%(now-recording_since,cfg["record_duration_s"])
        elif elapsed<cfg["stability"]["min_warmup_s"]: stage="预热中，还需 %.0fs"%(cfg["stability"]["min_warmup_s"]-elapsed)
        else: stage="等待判稳"
        util_text="--" if avg_util is None else "%.1f%%"%avg_util
        print("[状态] %s | 已收节点 %d/%d | 稳定窗口平均DCU利用率 %s | DCU解析失败节点 %d"%
              (stage,len(seen),len(roles),util_text,failures),flush=True)
        fixed_mode=args.no_wait_stable or stability.get("mode","auto")=="fixed_warmup"
        if not recording_since and not fixed_mode:
            warm_ok=elapsed>=stability["min_warmup_s"]
            window_ok=len(snap["full_nodes"])==len(roles); load_ok=(snap["cluster_util"] is not None and snap["cluster_util"]>=stability["min_cluster_dcu_util_pct"])
            util_ok=not snap["util_fail"]
            mark=lambda ok:"满足" if ok else "未满足"
            load_value="--" if snap["cluster_util"] is None else "%.1f%%"%snap["cluster_util"]
            parts=[]
            if stability["min_warmup_s"]>0:
                parts.append("预热:%s(%.0f/%ss)"%(mark(warm_ok),min(elapsed,stability["min_warmup_s"]),stability["min_warmup_s"]))
            parts.extend([
                "窗口:%s(%d/%d节点)"%(mark(window_ok),len(snap["full_nodes"]),len(roles)),
                "集群负载:%s(%s/≥%.1f%%)"%(mark(load_ok),load_value,stability["min_cluster_dcu_util_pct"]),
                "利用率稳定:%s(%d/%d节点)"%(mark(util_ok),len(snap["util_pass"]),len(roles)),
                "连续:%d/%d"%(stable_checks,stability["consecutive_checks"]),
            ])
            print("[判稳条件] "+" | ".join(parts),flush=True)
            if window_ok and snap["util_fail"]:
                print("[未满足节点] 利用率="+",".join(snap["util_fail"]),flush=True)
        last_status=now
    print("模型名称: %s\n输出目录: %s\n部署模式: %s\n节点分组: %s"%(model_name,outdir,mode,"; ".join("%s=[%s]"%(g,", ".join(ns)) for g,ns in active_groups.items())),flush=True)
    with open(complete_path,"w",newline="",encoding="utf-8-sig") as cf, open(core_path,"w",newline="",encoding="utf-8-sig") as coref, open(raw_path,"w",newline="",encoding="utf-8-sig") as f, open(ib_path,"w",newline="",encoding="utf-8-sig") as ibf:
        complete_writer=csv.DictWriter(cf,fieldnames=selected_fields(COMPLETE_FIELDS,metrics),extrasaction="ignore"); complete_writer.writeheader()
        core_writer=csv.DictWriter(coref,fieldnames=CORE_FIELDS); core_writer.writeheader()
        writer=csv.DictWriter(f,fieldnames=selected_fields(FIELDS,metrics),extrasaction="ignore"); writer.writeheader()
        ib_writer=csv.DictWriter(ibf,fieldnames=selected_fields(IB_FIELDS,metrics),extrasaction="ignore"); ib_writer.writeheader()
        while not stop.is_set():
            try: node,sample,err=q.get(timeout=.5)
            except queue.Empty:
                show_status()
                if not any(t.is_alive() for t in threads): break
                continue
            if sample is None:
                print(f"[{node}] {err}",file=sys.stderr); continue
            latest[node]=sample
            if node not in seen:
                seen.add(node); power_text="--" if sample.get("node_power_w") is None else "%.1fW"%sample["node_power_w"]
                cpu_power_text="--" if sample.get("cpu_power_w") is None else "%.1fW"%sample["cpu_power_w"]
                msg="[%s] 首包正常: DCU=%d, IB端口=%d, CPU功耗=%s, 整机功耗=%s"%(node,len(sample.get("dcus",[])),len(sample.get("ib",[])),cpu_power_text,power_text)
                cards=sample.get("dcus",[]); missing=[]
                if cards and metrics.get("dcu_memory") and not any(c.get("mem_used_mib") is not None for c in cards):missing.append("DCU显存")
                if cards and metrics.get("dcu_temperature") and not any(c.get("temp_c") is not None for c in cards):missing.append("DCU温度")
                if cards and metrics.get("dcu_clock") and not any(c.get("core_clock_mhz") is not None for c in cards):missing.append("DCU时钟")
                if metrics.get("cpu_power") and sample.get("cpu_power_w") is None:missing.append("CPU功耗")
                if metrics.get("node_power") and sample.get("node_power_w") is None:missing.append("整机功耗")
                if missing:msg+="; 未取到="+",".join(missing)+"（汇总显示--）"
                if sample.get("error"):msg+="; "+sample["error"]
                print(msg,flush=True)
            util=mean([x.get("util_pct") for x in sample.get("dcus",[])]); power=mean([x.get("power_w") for x in sample.get("dcus",[])])
            histories[node].append((sample.get("ts"),util,power))
            elapsed=time.time()-started; phase="recording" if recording_since else "warming"
            fixed_mode=args.no_wait_stable or stability.get("mode","auto")=="fixed_warmup"
            if fixed_mode and elapsed>=stability["min_warmup_s"]: recording_since=recording_since or time.time(); phase="recording"
            elif not recording_since and elapsed>=stability["min_warmup_s"] and time.time()-last_stability_check>=cfg["sample_interval_s"]:
                last_stability_check=time.time(); snap=stability_snapshot(); full=len(snap["full_nodes"])==len(roles); cluster_util=snap["cluster_util"]
                util_ok=not snap["util_fail"]
                ok=(full and cluster_util is not None and cluster_util>=stability["min_cluster_dcu_util_pct"] and util_ok)
                previous_checks=stable_checks; stable_checks=stable_checks+1 if ok else 0
                if ok:
                    print("[判稳进度] 本次满足，连续 %d/%d"%(stable_checks,stability["consecutive_checks"]),flush=True)
                elif previous_checks:
                    print("[判稳进度] 条件未满足，连续 %d/%d → 0/%d"%(previous_checks,stability["consecutive_checks"],stability["consecutive_checks"]),flush=True)
                if stable_checks>=stability["consecutive_checks"]:
                    recording_since=time.time(); phase="recording"; print(f"推理已判稳，开始记录 {cfg['record_duration_s']} 秒",flush=True)
            full=complete_row(sample,node,roles[node],started,phase); complete_rows.append(full); complete_writer.writerow(full); cf.flush()
            core_writer.writerow(core_row(full)); coref.flush()
            new=rows_for_sample(sample,node,roles[node],started,phase,err or "")
            rows.extend(new); writer.writerows(new); f.flush()
            new_ib=ib_rows_for_sample(sample,node,roles[node],started,phase)
            ib_rows.extend(new_ib); ib_writer.writerows(new_ib); ibf.flush()
            show_status()
            if recording_since and time.time()-recording_since>=cfg["record_duration_s"]: stop.set()
    stop.set(); summarize(rows,summary_path,metrics); summarize_ib(ib_rows,ib_summary_path,metrics)
    report=terminal_summary(complete_rows,metrics); log_path.write_text(report+"\n",encoding="utf-8")
    print("\n"+report)
    print(f"\n核心资源表: {core_path}\n完整宽表: {complete_path}\n终端汇总日志: {log_path}\nDCU 明细/汇总: {raw_path} / {summary_path}\nIB 明细/汇总: {ib_path} / {ib_summary_path}")


HOST_FIELDS = [
    "timestamp","node_timestamp","node_clock_offset_s","elapsed_s","role","node","route_state","route_event","phase","shared_phase","node_phase",
    "cpu_util_pct","cpu_user_pct","cpu_system_pct","cpu_iowait_pct","cpu_freq_avg_mhz","cpu_freq_max_mhz",
    "cpu_temp_avg_c","cpu_temp_max_c","cpu_power_w",
    "host_mem_used_gib","host_mem_total_gib","host_mem_util_pct","node_power_w","error",
]

NODE_DCU_FIELDS = [
    "timestamp","node_timestamp","node_clock_offset_s","elapsed_s","role","node","route_state","route_event","phase","shared_phase","node_phase","dcu_index",
    "dcu_util_pct","dcu_util_sample_age_s","dcu_mem_used_gib","dcu_mem_total_gib","dcu_mem_util_pct",
    "dcu_power_w","dcu_temp_c","dcu_temp_edge_c","dcu_temp_junction_c","dcu_temp_mem_c","dcu_temp_core_c","error",
]

ROUTE_EVENT_FIELDS = [
    "timestamp","confirmed_timestamp","elapsed_s","host","port","state","event","detail",
]

SUMMARY_FIELDS = ["role","node","category","device","metric","unit","samples","average","maximum"]


def iso_time(ts):
    return datetime.fromtimestamp(ts,timezone.utc).astimezone().isoformat(timespec="milliseconds")


def probe_route(cfg,outq,stop,started):
    probe=cfg.get("route_probe",{})
    if not probe.get("enabled",False):return
    host=str(probe.get("host","")).strip(); port=int(probe.get("port",0))
    if not host or not port:
        outq.put(("__route__",None,"路由端口监控已启用，但 host/port 未正确配置")); return
    interval=max(.1,float(probe.get("interval_s",1)))
    timeout=max(.05,float(probe.get("connect_timeout_s",.5)))
    success_need=max(1,int(probe.get("successes_to_reachable",1)))
    failure_need=max(1,int(probe.get("failures_to_unreachable",3)))
    state=None; streak_kind=None; streak=0; streak_started=None; last_detail=""
    while not stop.is_set():
        checked=time.time(); reachable=False; detail=""
        try:
            sock=socket.create_connection((host,port),timeout=timeout); sock.close(); reachable=True
        except Exception as exc: detail="%s: %s"%(type(exc).__name__,exc)
        # Ctrl+C 可能发生在 connect 调用期间；停止后不再生成任何新端口事件。
        if stop.is_set():break
        kind="reachable" if reachable else "unreachable"
        if streak_kind!=kind:
            streak_kind=kind; streak=1; streak_started=checked
        else:streak+=1
        last_detail=detail
        required=success_need if reachable else failure_need
        # 初始状态立即记录；后续切换按连续成功/失败次数消抖。
        should_emit=(state is None) or (kind!=state and streak>=required)
        if should_emit:
            observed=streak_started if state is not None else checked
            event={"timestamp":iso_time(observed),"confirmed_timestamp":iso_time(checked),
                   "elapsed_s":round(observed-started,3),"host":host,"port":port,"state":kind,
                   "event":("initial_"+kind if state is None else kind),"detail":last_detail}
            state=kind; outq.put(("__route__",event,None))
        stop.wait(interval)


def _stats(values):
    vals=[float(x) for x in values if x not in (None,"")]
    if not vals:return 0,None,None
    return len(vals),sum(vals)/len(vals),max(vals)


def _fresh_node_util_snapshots(rows,expected_cards=4):
    """从重复写入的缓存值中还原 showhcuutil 的真实刷新样本。"""
    grouped=defaultdict(dict)
    for row in rows:
        util=row.get("dcu_util_pct"); age=row.get("dcu_util_sample_age_s")
        if util in (None,"") or age in (None,""):continue
        measured=float(row["elapsed_s"])-float(age)
        bucket=round(measured*2)/2.0
        grouped[bucket][str(row.get("dcu_index"))]=float(util)
    return [(elapsed,sum(cards.values())/len(cards)) for elapsed,cards in sorted(grouped.items()) if len(cards)>=expected_cards]


def detect_steady_state(cfg,mode,active_groups,dcu_rows,total_elapsed):
    settings=cfg.get("steady_state",{})
    result={"status":"not_detected","start_elapsed_s":None,"end_elapsed_s":None,
            "duration_s":None,"start_confirmed":False,"end_confirmed":False,"reference_group":None,
            "reference_nodes":[],"fresh_samples":[],"settings":dict(settings)}
    requested=str(settings.get("reference_group","AUTO")).upper()
    if requested=="AUTO":
        if mode=="PD" and active_groups.get("D"):reference_group="D"
        elif mode=="IFB" and active_groups.get("IFB"):reference_group="IFB"
        elif active_groups.get("P"):reference_group="P"
        else:reference_group=next(iter(active_groups),"")
    else:reference_group=requested
    nodes=list(active_groups.get(reference_group,[])); result["reference_group"]=reference_group; result["reference_nodes"]=nodes
    if not nodes:
        result["reason"]="参考组没有生效节点"; return result
    expected_cards=max(1,int(cfg.get("expected_dcu_cards_per_node",4)))
    per_node={node:_fresh_node_util_snapshots(dcu_rows.get(node,[]),expected_cards) for node in nodes}
    if any(not samples for samples in per_node.values()):
        result["reason"]="参考组存在没有有效 DCU 利用率样本的节点"; return result
    anchor=max(nodes,key=lambda node:len(per_node[node])); tolerance=max(1.5,float(cfg.get("dcu_utilization_interval_s",5))*.45)
    series=[]
    for elapsed,util in per_node[anchor]:
        values=[util]; times=[elapsed]; complete=True
        for node in nodes:
            if node==anchor:continue
            nearest=min(per_node[node],key=lambda item:abs(item[0]-elapsed))
            if abs(nearest[0]-elapsed)>tolerance:complete=False; break
            times.append(nearest[0]); values.append(nearest[1])
        if complete:series.append({"elapsed_s":round(sum(times)/len(times),3),"util_pct":round(sum(values)/len(values),4)})
    dedup=[]
    for sample in series:
        if dedup and abs(sample["elapsed_s"]-dedup[-1]["elapsed_s"])<.75:
            dedup[-1]=sample
        else:dedup.append(sample)
    series=dedup; result["fresh_samples"]=series
    window=int(settings.get("window_fresh_samples",6)); confirmations=int(settings.get("confirm_windows",2))
    active=float(settings.get("active_threshold_pct",5)); max_change=float(settings.get("max_half_mean_change_pct",10))
    consecutive=0; confirm_index=None; reference_mean=None
    for index in range(window-1,len(series)):
        values=[sample["util_pct"] for sample in series[index-window+1:index+1]]; split=window//2
        left=sum(values[:split])/len(values[:split]); right=sum(values[split:])/len(values[split:])
        change=abs(right-left)/max(left,right,1)*100
        ok=min(left,right)>=active and change<=max_change
        consecutive=consecutive+1 if ok else 0
        if consecutive>=confirmations:
            confirm_index=index; reference_mean=sum(values)/len(values); break
    if confirm_index is None:
        result["reason"]="没有连续满足稳定窗口的 DCU 利用率样本"; return result
    band=max(3.0,reference_mean*max_change/100)
    start_index=confirm_index
    while start_index>0:
        previous=series[start_index-1]["util_pct"]
        if previous<active or abs(previous-reference_mean)>band:break
        start_index-=1
    idle=float(settings.get("idle_threshold_pct",2)); idle_needed=int(settings.get("idle_confirm_samples",2))
    low_count=0; end_index=None
    for index in range(confirm_index+1,len(series)):
        low_count=low_count+1 if series[index]["util_pct"]<=idle else 0
        if low_count>=idle_needed:
            first_low=index-low_count+1; candidate=first_low-1
            while candidate>=start_index and abs(series[candidate]["util_pct"]-reference_mean)>band:candidate-=1
            if candidate>=start_index:end_index=candidate
            break
    start_elapsed=series[start_index]["elapsed_s"]
    if end_index is not None:
        end_elapsed=series[end_index]["elapsed_s"]; status="detected"; end_confirmed=True
    else:
        last_in_band=max((i for i in range(start_index,len(series)) if abs(series[i]["util_pct"]-reference_mean)<=band),default=start_index)
        last_sample=series[-1]
        if last_in_band<len(series)-1 and (last_sample["util_pct"]<active or abs(last_sample["util_pct"]-reference_mean)>band):
            end_elapsed=series[last_in_band]["elapsed_s"]; status="steady_end_unconfirmed"; end_confirmed=False
        else:
            end_elapsed=total_elapsed; status="steady_open_at_stop"; end_confirmed=False
    result.update({"status":status,"start_elapsed_s":round(start_elapsed,3),"end_elapsed_s":round(end_elapsed,3),
                   "duration_s":round(max(0,end_elapsed-start_elapsed),3),"start_confirmed":True,"end_confirmed":end_confirmed,
                   "confirmed_at_elapsed_s":series[confirm_index]["elapsed_s"],"reference_mean_util_pct":round(reference_mean,4),
                   "stable_band_tolerance_pct":round(band,4)})
    return result


def detect_node_steady_states(cfg,roles,dcu_rows,total_elapsed):
    """每个节点使用自己的四卡平均利用率，独立计算稳态区间。"""
    results={}
    for node,role in roles.items():
        node_cfg={**cfg,"steady_state":{**cfg.get("steady_state",{}),"reference_group":"NODE"}}
        result=detect_steady_state(node_cfg,"NODE",{"NODE":[node]},{node:dcu_rows.get(node,[])},total_elapsed)
        result.update({"scope":"per_node","node":node,"role":role,"reference_group":role,"reference_nodes":[node]})
        results[node]=result
    return results


def _phase_for_elapsed(elapsed,steady):
    start=steady.get("start_elapsed_s"); end=steady.get("end_elapsed_s")
    if start is None or end is None:return "not_detected"
    if elapsed<start:return "before_steady"
    if elapsed<=end:return "steady"
    return "after_steady"


def mark_phases(host_rows,dcu_rows,steady,field="phase"):
    for rows_by_node in (host_rows,dcu_rows):
        for rows in rows_by_node.values():
            for row in rows:
                elapsed=float(row.get("elapsed_s",0))
                row[field]=_phase_for_elapsed(elapsed,steady)


def mark_node_phases(host_rows,dcu_rows,node_steady):
    for node,steady in node_steady.items():
        for rows_by_node in (host_rows,dcu_rows):
            for row in rows_by_node.get(node,[]):
                row["node_phase"]=_phase_for_elapsed(float(row.get("elapsed_s",0)),steady)


def steady_rows(rows,steady):
    return [row for row in rows if _phase_for_elapsed(float(row.get("elapsed_s",0)),steady)=="steady"]


def build_node_summary(role,node,host_rows,dcu_rows):
    result=[]
    host_specs=[
        ("CPU","host","cpu_utilization","%","cpu_util_pct","cpu_util_pct"),
        ("CPU","host","cpu_frequency","MHz","cpu_freq_avg_mhz","cpu_freq_max_mhz"),
        ("CPU","host","cpu_temperature","C","cpu_temp_avg_c","cpu_temp_max_c"),
        ("CPU","host","cpu_power","W","cpu_power_w","cpu_power_w"),
        ("Memory","host","memory_used","GiB","host_mem_used_gib","host_mem_used_gib"),
        ("Memory","host","memory_utilization","%","host_mem_util_pct","host_mem_util_pct"),
        ("Node","host","node_power","W","node_power_w","node_power_w"),
    ]
    for category,device,metric,unit,avg_key,max_key in host_specs:
        count,avg,_=_stats([r.get(avg_key) for r in host_rows]); _,_,maximum=_stats([r.get(max_key) for r in host_rows])
        result.append({"role":role,"node":node,"category":category,"device":device,"metric":metric,
                       "unit":unit,"samples":count,"average":round(avg,4) if avg is not None else "",
                       "maximum":round(maximum,4) if maximum is not None else ""})
    by_card=defaultdict(list)
    for row in dcu_rows:by_card[row.get("dcu_index")].append(row)
    specs=[("dcu_utilization","%","dcu_util_pct"),("vram_used","GiB","dcu_mem_used_gib"),
           ("vram_utilization","%","dcu_mem_util_pct"),("dcu_power","W","dcu_power_w"),
           ("dcu_temperature","C","dcu_temp_c")]
    for card,card_rows in sorted(by_card.items(),key=lambda x:str(x[0])):
        for metric,unit,key in specs:
            count,avg,maximum=_stats([r.get(key) for r in card_rows])
            result.append({"role":role,"node":node,"category":"DCU","device":"dcu%s"%card,"metric":metric,
                           "unit":unit,"samples":count,"average":round(avg,4) if avg is not None else "",
                           "maximum":round(maximum,4) if maximum is not None else ""})
    return result


def write_csv(path,fields,rows):
    with open(path,"w",newline="",encoding="utf-8-sig") as stream:
        writer=csv.DictWriter(stream,fieldnames=fields,extrasaction="ignore"); writer.writeheader(); writer.writerows(rows)


def make_node_svg(path,model,role,node,host_rows,dcu_rows,events,started,steady=None):
    colors=["#2563eb","#dc2626","#16a34a","#9333ea","#ea580c","#0891b2","#be123c","#4f46e5"]
    cards=sorted({r.get("dcu_index") for r in dcu_rows},key=str)
    def host_series(label,key,color_index=0):
        return [(label,colors[color_index%len(colors)],[(r["elapsed_s"],r.get(key)) for r in host_rows])]
    def card_series(key):
        return [("DCU%s"%card,colors[i%len(colors)],[(r["elapsed_s"],r.get(key)) for r in dcu_rows if r.get("dcu_index")==card]) for i,card in enumerate(cards)]
    panels=[
        ("CPU utilization","%",host_series("CPU","cpu_util_pct")),
        ("CPU frequency","MHz",host_series("CPU avg","cpu_freq_avg_mhz",0)+host_series("CPU max","cpu_freq_max_mhz",1)),
        ("CPU temperature","C",host_series("CPU avg","cpu_temp_avg_c",0)+host_series("CPU max","cpu_temp_max_c",1)),
        ("CPU power","W",host_series("CPU","cpu_power_w")),
        ("Host memory utilization","%",host_series("Memory","host_mem_util_pct")),
        ("Node power","W",host_series("Node","node_power_w")),
        ("DCU utilization (last-second HCU active ratio)","%",card_series("dcu_util_pct")),
        ("DCU VRAM utilization","%",card_series("dcu_mem_util_pct")),
        ("DCU power","W",card_series("dcu_power_w")),
        ("DCU junction temperature","C",card_series("dcu_temp_c")),
    ]
    all_elapsed=[float(r["elapsed_s"]) for r in host_rows]+[float(r["elapsed_s"]) for r in dcu_rows]
    xmax=max(all_elapsed) if all_elapsed else 1; xmax=max(1,xmax)
    is_full=bool(steady and steady.get("scope")=="full")
    steady_start=(float(steady["start_elapsed_s"]) if steady and steady.get("start_elapsed_s") is not None else None)
    steady_end=(float(steady["end_elapsed_s"]) if steady and steady.get("end_elapsed_s") is not None else None)
    width=1500; left=90; right=30; top=100; panel_h=205; plot_w=width-left-right
    height=top+panel_h*len(panels)+45; out=[]
    out.append('<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">'%(width,height,width,height))
    out.append('<rect width="100%%" height="100%%" fill="#ffffff"/>')
    out.append('<style>text{font-family:Arial,"Microsoft YaHei",sans-serif;fill:#1f2937}.axis{stroke:#9ca3af;stroke-width:1}.grid{stroke:#e5e7eb;stroke-width:1}.series{fill:none;stroke-width:1.8}.event{stroke-width:1.4;stroke-dasharray:5 4}.steady-boundary{stroke:#7c3aed;stroke-width:1.6;stroke-dasharray:7 4}.steady-zone{fill:#ede9fe;opacity:.45}</style>')
    out.append('<text x="%d" y="34" font-size="24" font-weight="700">%s / %s / %s</text>'%(left,html.escape(model),html.escape(role),html.escape(node)))
    scope_label="node-specific steady interval" if steady and steady.get("scope")=="per_node" else "shared steady interval"
    if is_full:scope_label="full run (no steady filtering)"
    out.append('<text x="%d" y="62" font-size="13">Purple band: %s; green/red lines: route reachable/unreachable. Legends show selected average / maximum.</text>'%(left,scope_label))
    for pi,(title,unit,series) in enumerate(panels):
        y0=top+pi*panel_h; plot_top=y0+28; plot_bottom=y0+160; plot_h=plot_bottom-plot_top
        values=[float(v) for _,_,points in series for _,v in points if v not in (None,"")]
        ymax=max(values) if values else 1
        if unit=="%":ymax=max(100,ymax)
        else:ymax=max(1,ymax*1.1)
        out.append('<text x="%d" y="%d" font-size="16" font-weight="700">%s (%s)</text>'%(left,y0+18,html.escape(title),html.escape(unit)))
        for gi in range(5):
            gy=plot_bottom-plot_h*gi/4; value=ymax*gi/4
            out.append('<line class="grid" x1="%d" y1="%.1f" x2="%d" y2="%.1f"/>'%(left,gy,width-right,gy))
            out.append('<text x="%d" y="%.1f" font-size="11" text-anchor="end">%.1f</text>'%(left-8,gy+4,value))
        out.append('<line class="axis" x1="%d" y1="%d" x2="%d" y2="%d"/>'%(left,plot_bottom,width-right,plot_bottom))
        if steady_start is not None and steady_end is not None and not is_full:
            ss=max(0,min(xmax,steady_start)); se=max(ss,min(xmax,steady_end))
            sx=left+plot_w*ss/xmax; ex=left+plot_w*se/xmax
            out.append('<rect class="steady-zone" x="%.1f" y="%d" width="%.1f" height="%d"/>'%(sx,plot_top,max(0,ex-sx),plot_h))
            out.append('<line class="steady-boundary" x1="%.1f" y1="%d" x2="%.1f" y2="%d"/>'%(sx,plot_top,sx,plot_bottom))
            out.append('<line class="steady-boundary" x1="%.1f" y1="%d" x2="%.1f" y2="%d"/>'%(ex,plot_top,ex,plot_bottom))
            if pi==0:
                out.append('<text x="%.1f" y="%d" font-size="10" fill="#7c3aed">steady start %.1fs</text>'%(sx+3,plot_top+12,ss))
                out.append('<text x="%.1f" y="%d" font-size="10" fill="#7c3aed" text-anchor="end">steady end %.1fs</text>'%(ex-3,plot_top+12,se))
        for event in events:
            ex=left+plot_w*min(xmax,max(0,float(event.get("elapsed_s",0))))/xmax
            color="#16a34a" if event.get("state")=="reachable" else "#dc2626"
            out.append('<line class="event" stroke="%s" x1="%.1f" y1="%d" x2="%.1f" y2="%d"/>'%(color,ex,plot_top,ex,plot_bottom))
            if pi==0:
                label="%s %s"%(event.get("state"),str(event.get("timestamp",""))[11:19])
                out.append('<text x="%.1f" y="%d" font-size="10" fill="%s" transform="rotate(-45 %.1f %d)">%s</text>'%(ex,plot_top-3,color,ex,plot_top-3,html.escape(label)))
        legend_x=left
        for label,color,points in series:
            valid=[(float(t),float(v)) for t,v in points if v not in (None,"")]
            if valid:
                coords=["%.1f,%.1f"%(left+plot_w*t/xmax,plot_bottom-plot_h*v/ymax) for t,v in valid]
                out.append('<polyline class="series" stroke="%s" points="%s"/>'%(color," ".join(coords)))
                stat_vals=[v for t,v in valid if steady_start is not None and steady_end is not None and steady_start<=t<=steady_end]
                stat_label="full" if is_full else "steady"
                legend=("%s %s avg=%.2f max=%.2f"%(label,stat_label,sum(stat_vals)/len(stat_vals),max(stat_vals)) if stat_vals else "%s no selected data"%label)
            else:legend="%s no data"%label
            out.append('<line stroke="%s" stroke-width="3" x1="%d" y1="%d" x2="%d" y2="%d"/>'%(color,legend_x,y0+188,legend_x+18,y0+188))
            out.append('<text x="%d" y="%d" font-size="11">%s</text>'%(legend_x+23,y0+192,html.escape(legend)))
            legend_x+=max(190,8*len(legend))
        for xi in range(5):
            elapsed=xmax*xi/4; x=left+plot_w*xi/4; label=datetime.fromtimestamp(started+elapsed).strftime("%H:%M:%S")
            out.append('<text x="%.1f" y="%d" font-size="10" text-anchor="middle">%s</text>'%(x,plot_bottom+15,label))
    out.append('</svg>'); path.write_text("\n".join(out),encoding="utf-8")


def _dashboard_node_metrics(host_rows,dcu_rows):
    def metric(rows,avg_key,max_key=None):
        _,avg,_=_stats([row.get(avg_key) for row in rows])
        _,_,maximum=_stats([row.get(max_key or avg_key) for row in rows]); return avg,maximum
    result={
        "cpu_util":metric(host_rows,"cpu_util_pct"),
        "cpu_temp":metric(host_rows,"cpu_temp_avg_c","cpu_temp_max_c"),
        "cpu_power":metric(host_rows,"cpu_power_w"),
        "memory_used":metric(host_rows,"host_mem_used_gib"),
        "memory_util":metric(host_rows,"host_mem_util_pct"),
        "node_power":metric(host_rows,"node_power_w"),
        "dcu_util":metric(dcu_rows,"dcu_util_pct"),
        "vram_util":metric(dcu_rows,"dcu_mem_util_pct"),
        "dcu_temp":metric(dcu_rows,"dcu_temp_c"),
    }
    power_by_sample=defaultdict(list)
    for row in dcu_rows:
        if row.get("dcu_power_w") not in (None,""):power_by_sample[row.get("elapsed_s")].append(float(row["dcu_power_w"]))
    totals=[sum(values) for values in power_by_sample.values() if values]
    _,avg,maximum=_stats(totals); result["dcu_power_total"]=(avg,maximum)
    return result


def make_dashboard(path,model,node_infos,full_host,full_dcu,shared_host,shared_dcu,node_host,node_dcu,shared_steady,node_steady):
    role_order=[]; overview=[]
    for role,node,full_svg,shared_svg,node_svg in node_infos:
        if role not in role_order:role_order.append(role)
        overview.append({"role":role,"node":node,"full_svg":full_svg,"shared_svg":shared_svg,"node_svg":node_svg,
                         "metrics":{
                             "full":_dashboard_node_metrics(full_host.get(node,[]),full_dcu.get(node,[])),
                             "shared":_dashboard_node_metrics(shared_host.get(node,[]),shared_dcu.get(node,[])),
                             "per-node":_dashboard_node_metrics(node_host.get(node,[]),node_dcu.get(node,[])),
                         }})
    metric_defs=[
        ("cpu_util","CPU利用率","%"),("cpu_temp","CPU温度","°C"),("cpu_power","CPU功耗","W"),
        ("memory_used","内存占用","GiB"),("memory_util","内存利用率","%"),
        ("dcu_util","DCU利用率","%"),("vram_util","显存利用率","%"),
        ("dcu_power_total","DCU总功耗","W"),("dcu_temp","DCU温度","°C"),("node_power","整机功耗","W"),
    ]
    def number(value):return "--" if value is None else ("%.2f"%value)
    header="".join('<th>%s<small>%s</small></th>'%(html.escape(label),html.escape(unit)) for _,label,unit in metric_defs)
    table_rows=[]
    for item in overview:
        cells=[]
        for key,_,_ in metric_defs:
            scope_values=[]
            for scope in ("full","shared","per-node"):
                avg,maximum=item["metrics"][scope].get(key,(None,None))
                scope_values.append('<span class="scope-content" data-scope="%s"><span class="avg">%s</span><span class="max">%s</span></span>'%
                                    (scope,number(avg),number(maximum)))
            cells.append('<td>%s</td>'%"".join(scope_values))
        table_rows.append('<tr><th><span class="role role-%s">%s</span> %s</th>%s</tr>'%
                          (html.escape(item["role"].lower()),html.escape(item["role"]),html.escape(item["node"]),"".join(cells)))
    comparison_defs=[definition for definition in metric_defs if definition[0] in
                     ("cpu_util","cpu_power","memory_util","dcu_util","vram_util","dcu_power_total","node_power")]
    comparison_cards=[]
    for key,label,unit in comparison_defs:
        scope_blocks=[]
        for scope in ("full","shared","per-node"):
            scale=max([item["metrics"][scope].get(key,(None,None))[1] or 0 for item in overview]+[1])
            rows=[]
            for item in overview:
                avg,maximum=item["metrics"][scope].get(key,(None,None)); avg_width=0 if avg is None else min(100,avg*100/scale)
                max_left=0 if maximum is None else min(100,maximum*100/scale)
                rows.append('''<div class="bar-row"><span class="bar-node"><i class="role-dot role-%s"></i>%s</span><div class="bar-track"><span class="bar-fill role-bg-%s" style="width:%.3f%%"></span>%s</div><span class="bar-value">%s / %s</span></div>'''%
                            (html.escape(item["role"].lower()),html.escape(item["node"]),html.escape(item["role"].lower()),avg_width,
                             "" if maximum is None else '<i class="maximum-marker" style="left:%.3f%%"></i>'%max_left,number(avg),number(maximum)))
            scope_blocks.append('<div class="scope-content" data-scope="%s">%s</div>'%(scope,"".join(rows)))
        comparison_cards.append('<article class="compare-card"><h3>%s <small>%s</small></h3><div class="compare-legend">平均值条形 · <b>◆</b> 最大值</div>%s</article>'%
                                (html.escape(label),html.escape(unit),"".join(scope_blocks)))
    role_buttons=[]; node_tab_groups=[]; detail_panels=[]
    for role in role_order:
        role_items=[item for item in overview if item["role"]==role]
        role_buttons.append('<button type="button" class="role-tab" data-role="%s">%s（%d）</button>'%(html.escape(role),html.escape(role),len(role_items)))
        node_buttons=[]
        for item in role_items:
            detail_id="detail-%d"%overview.index(item)
            node_buttons.append('<button type="button" class="node-tab" data-detail="%s">%s</button>'%(detail_id,html.escape(item["node"])))
            scoped_images=[]
            for scope,svg in (("full",item["full_svg"]),("shared",item["shared_svg"]),("per-node",item["node_svg"])):
                scoped_images.append('''<div class="scope-content chart-scope" data-scope="%s"><a class="svg-link" href="%s" target="_blank">打开原尺寸 SVG</a><img src="%s" loading="lazy" alt="%s %s monitoring chart"></div>'''%
                                     (scope,html.escape(svg),html.escape(svg),html.escape(role),html.escape(item["node"])))
            detail_panels.append('''<article id="%s" class="detail-panel" data-role="%s"><div class="detail-title"><div><span class="role role-%s">%s</span><h3>%s 完整时间曲线</h3></div></div>%s</article>'''%
                                 (detail_id,html.escape(role),html.escape(role.lower()),html.escape(role),html.escape(item["node"]),"".join(scoped_images)))
        node_tab_groups.append('<div class="node-tabs" data-role="%s">%s</div>'%(html.escape(role),"".join(node_buttons)))
    def describe_steady(steady,prefix):
        if steady and steady.get("start_elapsed_s") is not None:
            end_note="已确认" if steady.get("end_confirmed") else "未确认"
            return "%s %.1fs–%.1fs · 持续 %.1fs · 结束%s"%(prefix,steady["start_elapsed_s"],steady["end_elapsed_s"],steady.get("duration_s",0),end_note)
        return "%s未检测到稳态；汇总保持为空"%prefix
    shared_summary=describe_steady(shared_steady,"统一稳态")
    full_summary="无稳态判断 · 使用脚本从启动到结束的全部有效样本"
    detected=sum(1 for result in node_steady.values() if result.get("start_elapsed_s") is not None)
    node_summary="每节点独立稳态 · 已检测 %d/%d 个节点；各节点使用自己的时间区间"%(detected,len(node_steady))
    replacements={
        "__TITLE__":html.escape(model),"__NODE_COUNT__":str(len(overview)),"__ROLE_BUTTONS__":"".join(role_buttons),
        "__TABLE_HEADER__":header,"__TABLE_ROWS__":"".join(table_rows),"__COMPARE_CARDS__":"".join(comparison_cards),
        "__NODE_TABS__":"".join(node_tab_groups),"__DETAIL_PANELS__":"".join(detail_panels),
        "__FULL_SUMMARY__":html.escape(full_summary),"__SHARED_SUMMARY__":html.escape(shared_summary),"__NODE_SUMMARY__":html.escape(node_summary),
    }
    doc='''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>__TITLE__ 监控总览</title>
<style>
:root{--ink:#0b1f33;--muted:#5b7083;--line:#cbd8e4;--paper:#f4f7fa;--panel:#fff;--blue:#1769aa;--cyan:#008f95;--orange:#e05a24;--green:#16845b;--purple:#7857a8}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--paper);color:var(--ink);font-family:"Microsoft YaHei UI","Microsoft YaHei",Arial,sans-serif}[hidden]{display:none!important}.page{width:min(1680px,100%);margin:auto;padding:20px 24px 44px}.masthead{display:flex;justify-content:space-between;gap:24px;align-items:flex-end;padding:22px 0 18px;border-bottom:3px solid var(--ink)}.eyebrow{font:700 12px Consolas,monospace;letter-spacing:.16em;color:var(--cyan)}h1{font-size:30px;margin:6px 0 0}.masthead p{margin:0;color:var(--muted)}.scope-switch{display:flex;gap:8px;align-items:center;padding:12px 0 4px}.scope-switch strong{margin-right:4px}.scope-button{appearance:none;border:1px solid #8ca4b7;background:#fff;color:var(--ink);padding:9px 15px;cursor:pointer;font-weight:700}.scope-button.active{background:var(--ink);border-color:var(--ink);color:#fff}.anchor-nav{position:sticky;top:0;z-index:8;display:flex;gap:8px;padding:10px 0;background:rgba(244,247,250,.96);border-bottom:1px solid var(--line)}.anchor-nav a{padding:7px 11px;color:var(--blue);text-decoration:none;font-weight:700}.section{margin-top:28px}.section-head{display:flex;justify-content:space-between;align-items:end;gap:20px;margin-bottom:12px}.section h2{font-size:21px;margin:0}.section-head p{margin:0;color:var(--muted);font-size:13px}.table-wrap{overflow:auto;background:var(--panel);border:1px solid var(--line)}table{width:100%;min-width:1450px;border-collapse:collapse;font:13px Consolas,"Microsoft YaHei UI",monospace}th,td{padding:10px 9px;border-bottom:1px solid #e5edf3;text-align:right;white-space:nowrap}thead th{position:sticky;top:0;background:#eaf1f6;color:#243c50}thead th:first-child,tbody th{text-align:left;position:sticky;left:0;background:#fff;z-index:2}thead th:first-child{z-index:3;background:#eaf1f6}th small{display:block;color:var(--muted);font-weight:400}.avg{color:var(--ink)}.max{color:var(--orange);margin-left:7px}.max:before{content:"/ ";color:#9aa9b5}.role{display:inline-grid;place-items:center;min-width:34px;padding:3px 7px;margin-right:6px;border-radius:3px;color:#fff;font:700 12px Consolas,monospace}.role-p,.role-bg-p{background:var(--blue)}.role-d,.role-bg-d{background:var(--cyan)}.role-ifb,.role-bg-ifb{background:var(--purple)}.compare-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.compare-card{background:#fff;border:1px solid var(--line);padding:15px}.compare-card h3{margin:0;font-size:16px}.compare-card h3 small{color:var(--muted)}.compare-legend{margin:4px 0 12px;color:var(--muted);font-size:11px}.bar-row{display:grid;grid-template-columns:92px minmax(120px,1fr) 112px;gap:9px;align-items:center;margin:8px 0;font:12px Consolas,monospace}.bar-node{overflow:hidden;text-overflow:ellipsis}.role-dot{display:inline-block;width:7px;height:7px;margin-right:6px;border-radius:50%}.role-dot.role-p{background:var(--blue)}.role-dot.role-d{background:var(--cyan)}.role-dot.role-ifb{background:var(--purple)}.bar-track{height:13px;background:#e7eef3;position:relative}.bar-fill{display:block;height:100%;min-width:1px}.maximum-marker{position:absolute;top:-3px;transform:translateX(-50%);color:var(--orange)}.maximum-marker:after{content:"◆";font-style:normal;font-size:13px}.bar-value{text-align:right;color:#334e62}.detail-controls{background:#eaf1f6;border:1px solid var(--line);padding:12px}.role-tabs,.node-tabs{display:flex;flex-wrap:wrap;gap:8px}.node-tabs{margin-top:9px}.role-tab,.node-tab{appearance:none;border:1px solid #9fb2c1;background:#fff;color:var(--ink);padding:7px 12px;cursor:pointer;font:700 13px "Microsoft YaHei UI",sans-serif}.role-tab.active,.node-tab.active,.scope-button:focus-visible,.role-tab:focus-visible,.node-tab:focus-visible,a:focus-visible{outline:3px solid #f3a45f;outline-offset:2px}.role-tab.active,.node-tab.active{background:var(--ink);border-color:var(--ink);color:#fff}.detail-panel{display:none;margin-top:12px;background:#fff;border:1px solid var(--line);padding:14px}.detail-panel.active{display:block}.detail-title{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}.detail-title>div{display:flex;align-items:center}.detail-title h3{display:inline;margin:0;font-size:18px}.svg-link{display:block;margin:-40px 0 14px auto;width:max-content;color:var(--blue);font-weight:700;text-decoration:none}.detail-panel img{display:block;width:100%;height:auto;border:1px solid #e3ebf1}footer{margin-top:24px;color:var(--muted);font-size:12px}@media(max-width:900px){.page{padding:12px}.masthead{display:block}.masthead p{margin-top:9px}.scope-switch{align-items:stretch;flex-direction:column}.compare-grid{grid-template-columns:1fr}.section-head{display:block}.section-head p{margin-top:5px}.bar-row{grid-template-columns:76px 1fr 92px}.detail-title{align-items:flex-start;gap:10px}.svg-link{margin:0 0 10px}}
.role-tab.active:not(:focus-visible),.node-tab.active:not(:focus-visible){outline:none}
</style></head><body><main class="page"><header class="masthead"><div><span class="eyebrow">SUPERNODE RESOURCE REPORT</span><h1>__TITLE__ 监控总览</h1></div><p>__NODE_COUNT__ 个计算节点<br><span class="scope-content" data-scope="full">__FULL_SUMMARY__</span><span class="scope-content" data-scope="shared">__SHARED_SUMMARY__</span><span class="scope-content" data-scope="per-node">__NODE_SUMMARY__</span></p></header><div class="scope-switch" role="group" aria-label="统计口径"><strong>统计口径</strong><button type="button" class="scope-button" data-select-scope="full">无稳态判断</button><button type="button" class="scope-button" data-select-scope="shared">统一稳态区间</button><button type="button" class="scope-button" data-select-scope="per-node">每节点独立区间</button></div><nav class="anchor-nav"><a href="#overview">节点汇总</a><a href="#compare">跨节点比较</a><a href="#details">时间曲线</a></nav>
<section id="overview" class="section"><div class="section-head"><h2>节点汇总矩阵</h2><p><span class="scope-content" data-scope="full">使用脚本全程全部有效样本</span><span class="scope-content" data-scope="shared">所有节点使用同一个稳态区间</span><span class="scope-content" data-scope="per-node">每个节点使用自己检测出的稳态区间</span>；表中为平均值 / 最大值</p></div><div class="table-wrap"><table><thead><tr><th>角色 / 节点</th>__TABLE_HEADER__</tr></thead><tbody>__TABLE_ROWS__</tbody></table></div></section>
<section id="compare" class="section"><div class="section-head"><h2>跨节点关键指标</h2><p>彩色条为平均值，橙色菱形为最大值</p></div><div class="compare-grid">__COMPARE_CARDS__</div></section>
<section id="details" class="section"><div class="section-head"><h2>单节点完整时间曲线</h2><p>一次显示一个节点，保持原图文字清晰</p></div><div class="detail-controls"><div class="role-tabs">__ROLE_BUTTONS__</div>__NODE_TABS__</div>__DETAIL_PANELS__</section>
<footer>无稳态判断使用脚本全程数据；按钮会同时切换汇总数值和曲线统计区间。路由端口事件单独标记，DCU利用率统一采用最近1秒 HCU active ratio。</footer></main>
<script>(function(){const scopeButtons=[...document.querySelectorAll('[data-select-scope]')],scopeContents=[...document.querySelectorAll('.scope-content')],roleButtons=[...document.querySelectorAll('.role-tab')],nodeGroups=[...document.querySelectorAll('.node-tabs')],nodeButtons=[...document.querySelectorAll('.node-tab')],panels=[...document.querySelectorAll('.detail-panel')];function showScope(scope){scopeButtons.forEach(b=>b.classList.toggle('active',b.dataset.selectScope===scope));scopeContents.forEach(el=>el.hidden=el.dataset.scope!==scope)}function showNode(id){nodeButtons.forEach(b=>b.classList.toggle('active',b.dataset.detail===id));panels.forEach(p=>p.classList.toggle('active',p.id===id))}function showRole(role){roleButtons.forEach(b=>b.classList.toggle('active',b.dataset.role===role));nodeGroups.forEach(g=>g.style.display=g.dataset.role===role?'flex':'none');const first=nodeButtons.find(b=>b.closest('.node-tabs').dataset.role===role);if(first)showNode(first.dataset.detail)}scopeButtons.forEach(b=>b.addEventListener('click',()=>showScope(b.dataset.selectScope)));roleButtons.forEach(b=>b.addEventListener('click',()=>showRole(b.dataset.role)));nodeButtons.forEach(b=>b.addEventListener('click',()=>showNode(b.dataset.detail)));showScope('full');if(roleButtons[0])showRole(roleButtons[0].dataset.role)})();</script></body></html>'''
    for key,value in replacements.items():doc=doc.replace(key,value)
    path.write_text(doc,encoding="utf-8")


def resolve_deployment_groups(cfg):
    deployment=cfg.get("deployment",{}); mode=str(deployment.get("mode","PD")).strip().upper()
    groups={str(role).strip().upper():nodes for role,nodes in deployment.get("groups",{}).items()}
    if mode=="PD":wanted=("P","D")
    elif mode=="IFB":wanted=("IFB",)
    else:raise SystemExit("deployment.mode 仅支持 PD 或 IFB")
    active={role:[str(node).strip() for node in groups.get(role,[]) if str(node).strip()] for role in wanted}
    active={role:nodes for role,nodes in active.items() if nodes}
    roles={node:role for role,nodes in active.items() for node in nodes}
    if sum(len(nodes) for nodes in active.values())!=len(roles):raise SystemExit("节点配置重复：同一节点不能重复或同时属于多个生效分组")
    if not roles:
        expected="P/D" if mode=="PD" else "IFB"
        raise SystemExit("部署模式为 %s，但 deployment.groups.%s 没有配置节点"%(mode,expected))
    return mode,active,roles


def node_health_text(roles,node_health,cfg,now,started):
    """汇总监控采集链路健康度；不依据模型负载或利用率高低判断。"""
    expected=int(cfg.get("expected_dcu_cards_per_node",4))
    sample_timeout=max(10.0,float(cfg.get("sample_interval_s",2))*4)
    util_interval=float(cfg.get("dcu_utilization_interval_s",5))
    util_timeout=max(15.0,util_interval*3)
    util_grace=max(10.0,util_interval*2+2)
    reasons={}; healthy_by_role=defaultdict(int); total_by_role=defaultdict(int)
    for node,role in roles.items():
        total_by_role[role]+=1; state=node_health.get(node,{}) ; node_reasons=[]
        received=state.get("last_received")
        if received is None:node_reasons.append("未收到首包")
        else:
            age=max(0.0,now-received)
            if age>sample_timeout:node_reasons.append("节点采样超时%.0fs"%age)
            card_count=state.get("dcu_count")
            if card_count is not None and card_count!=expected:node_reasons.append("DCU=%s/%s"%(card_count,expected))
            if state.get("last_error"):node_reasons.append("采集报错")
        if cfg.get("dcu_utilization_command") and now-started>=util_grace:
            util_received=state.get("util_received")
            if util_received is None:node_reasons.append("DCU利用率未就绪")
            elif now-util_received>util_timeout:node_reasons.append("DCU利用率超时%.0fs"%(now-util_received))
            if state.get("util_error"):node_reasons.append("DCU利用率报错")
        if node_reasons:reasons[node]="/".join(dict.fromkeys(node_reasons))
        else:healthy_by_role[role]+=1
    role_order=[]
    for role in roles.values():
        if role not in role_order:role_order.append(role)
    groups=", ".join("%s %d/%d正常"%(role,healthy_by_role[role],total_by_role[role]) for role in role_order)
    healthy=sum(healthy_by_role.values()); total=len(roles)
    if healthy==total:return "节点采集 %d/%d正常（全部正常；%s）"%(healthy,total,groups),True
    details=", ".join("%s(%s)"%(node,reasons[node]) for node in roles if node in reasons)
    return "节点采集 %d/%d正常（%s） | 异常: %s"%(healthy,total,groups,details),False


def main():
    ap=argparse.ArgumentParser(description="PD/IFB 服务全生命周期资源监控")
    ap.add_argument("--config",default="monitor_config.jsonc")
    ap.add_argument("--model",help="覆盖配置中的模型名称")
    ap.add_argument("--output-dir",default="results")
    ap.add_argument("--duration",type=float,help="本次固定监控秒数；0 表示直到 Ctrl+C")
    args=ap.parse_args(); cfg=load_config(args.config)
    model=(args.model if args.model is not None else cfg.get("model_name","unnamed_model")).strip() or "unnamed_model"
    safe_model="".join(c if c.isalnum() or c in "-_." else "_" for c in model)[:80] or "unnamed_model"
    mode,active_groups,roles=resolve_deployment_groups(cfg)
    duration=float(args.duration if args.duration is not None else cfg.get("monitor_duration_s",0))
    stamp=datetime.now().strftime("%Y%m%d_%H%M%S"); outdir=Path(args.output_dir)/(safe_model+"_run_"+stamp); outdir.mkdir(parents=True)
    cfg["model_name"]=model; (outdir/"effective_config.json").write_text(json.dumps(cfg,ensure_ascii=False,indent=2),encoding="utf-8")
    q=queue.Queue(); stop=threading.Event(); started=time.time(); threads=[]; node_threads=[]; seen=set(); route_state="unknown"
    events=[]; event_seq=0; tagged={node:0 for node in roles}; host_rows={node:[] for node in roles}; dcu_rows={node:[] for node in roles}
    node_health={node:{"last_received":None,"dcu_count":None,"last_error":"","util_received":None,"util_error":""} for node in roles}
    latest_active={}
    signal.signal(signal.SIGINT,lambda *_:stop.set())
    print("模型名称: %s\n输出目录: %s\n部署模式: %s\n节点分组: %s"%(model,outdir,mode,"; ".join("%s=[%s]"%(r,", ".join(ns)) for r,ns in active_groups.items())),flush=True)
    probe=cfg.get("route_probe",{})
    if probe.get("enabled",False):print("路由端口监控: %s:%s，每 %ss 探测一次"%(probe.get("host"),probe.get("port"),probe.get("interval_s",1)),flush=True)
    else:print("路由端口监控: 未启用",flush=True)
    with contextlib.ExitStack() as stack:
        handles={}; writers={}
        for node,role in roles.items():
            ndir=outdir/role/node; ndir.mkdir(parents=True)
            host_stream=stack.enter_context(open(ndir/"host.csv","w",newline="",encoding="utf-8-sig"))
            dcu_stream=stack.enter_context(open(ndir/"dcu_cards.csv","w",newline="",encoding="utf-8-sig"))
            hw=csv.DictWriter(host_stream,fieldnames=HOST_FIELDS); dw=csv.DictWriter(dcu_stream,fieldnames=NODE_DCU_FIELDS)
            hw.writeheader(); dw.writeheader(); handles[node]=(host_stream,dcu_stream); writers[node]=(hw,dw)
        event_stream=stack.enter_context(open(outdir/"route_events.csv","w",newline="",encoding="utf-8-sig"))
        event_writer=csv.DictWriter(event_stream,fieldnames=ROUTE_EVENT_FIELDS); event_writer.writeheader()
        for node in roles:
            thread=threading.Thread(target=spawn_node,args=(node,cfg,q,stop),daemon=True); thread.start(); threads.append(thread); node_threads.append(thread)
            if cfg.get("dcu_utilization_command"):
                util_thread=threading.Thread(target=spawn_dcu_util,args=(node,cfg,q,stop),daemon=True); util_thread.start(); threads.append(util_thread)
        if probe.get("enabled",False):
            thread=threading.Thread(target=probe_route,args=(cfg,q,stop,started),daemon=True); thread.start(); threads.append(thread)
        last_status=started; first_packet_check_printed=False
        while not stop.is_set():
            if duration>0 and time.time()-started>=duration:stop.set(); break
            now=time.time()
            if now-last_status>=10:
                health_text,_=node_health_text(roles,node_health,cfg,now,started)
                print("[状态] 已运行 %.0fs | %s | 路由端口 %s"%(now-started,health_text,route_state),flush=True)
                last_status=now
            try:node,sample,err=q.get(timeout=.5)
            except queue.Empty:
                if node_threads and not any(t.is_alive() for t in node_threads):break
                continue
            if node.startswith("__dcuutil__:"):
                target=node.split(":",1)[1]
                if sample is None:
                    node_health[target]["util_error"]=str(err or "未知错误")
                    print("[%s/DCU最近1秒利用率] %s"%(target,err),file=sys.stderr,flush=True)
                else:
                    util_received=time.time(); latest_active[target]={"received":util_received,"cards":sample.get("cards",[])}
                    node_health[target]["util_received"]=util_received; node_health[target]["util_error"]=""
                continue
            if node=="__route__":
                if sample is None:print("[路由端口] "+str(err),file=sys.stderr,flush=True); continue
                route_state=sample["state"]; events.append(sample); event_seq+=1; event_writer.writerow(sample); event_stream.flush()
                print("[路由端口] %s %s:%s，观测=%s，确认=%s"%(sample["state"],sample["host"],sample["port"],sample["timestamp"],sample["confirmed_timestamp"]),flush=True)
                continue
            if sample is None:
                if node in node_health:node_health[node]["last_error"]=str(err or "未知错误")
                print("[%s] %s"%(node,err),file=sys.stderr,flush=True); continue
            active=latest_active.get(node)
            if active:
                supplemental=[]
                for card in active["cards"]:supplemental.append({"index":card.get("index"),"active_util_pct":card.get("util_pct")})
                sample["dcus"]=merge_dcu_cards(sample.get("dcus",[]),supplemental)
            role=roles[node]; seen.add(node); received=time.time(); node_ts=sample.get("ts",received); elapsed=round(received-started,3)
            node_health[node].update({"last_received":received,"dcu_count":len(sample.get("dcus",[])),"last_error":str(sample.get("error","") or "")})
            route_event=""
            if event_seq>tagged[node] and events:route_event=events[-1]["event"]; tagged[node]=event_seq
            timing={"timestamp":iso_time(received),"node_timestamp":iso_time(node_ts),"node_clock_offset_s":round(node_ts-received,6),"elapsed_s":elapsed}
            host_row={**timing,"role":role,"node":node,"route_state":route_state,"route_event":route_event,
                      "phase":"unclassified","shared_phase":"unclassified","node_phase":"unclassified",
                      "cpu_util_pct":sample.get("cpu_util_pct"),"cpu_user_pct":sample.get("cpu_user_pct"),"cpu_system_pct":sample.get("cpu_system_pct"),
                      "cpu_iowait_pct":sample.get("cpu_iowait_pct"),"cpu_temp_avg_c":sample.get("cpu_temp_avg_c"),"cpu_temp_max_c":sample.get("cpu_temp_max_c"),
                      "cpu_freq_avg_mhz":sample.get("cpu_freq_avg_mhz"),"cpu_freq_max_mhz":sample.get("cpu_freq_max_mhz"),
                      "cpu_power_w":sample.get("cpu_power_w"),"host_mem_used_gib":sample.get("host_mem_used_mib")/1024 if sample.get("host_mem_used_mib") is not None else None,
                      "host_mem_total_gib":sample.get("host_mem_total_mib")/1024 if sample.get("host_mem_total_mib") is not None else None,
                      "host_mem_util_pct":sample.get("host_mem_util_pct"),"node_power_w":sample.get("node_power_w"),"error":sample.get("error","")}
            hw,dw=writers[node]; host_rows[node].append(host_row); hw.writerow(host_row)
            for card in sample.get("dcus",[]):
                drow={**timing,"role":role,"node":node,"route_state":route_state,"route_event":route_event,
                      "phase":"unclassified","shared_phase":"unclassified","node_phase":"unclassified",
                      "dcu_index":card.get("index"),"dcu_util_pct":card.get("active_util_pct"),
                      "dcu_util_sample_age_s":round(received-active["received"],3) if active else None,
                      "dcu_mem_used_gib":card.get("mem_used_mib")/1024 if card.get("mem_used_mib") is not None else None,
                      "dcu_mem_total_gib":card.get("mem_total_mib")/1024 if card.get("mem_total_mib") is not None else None,
                      "dcu_mem_util_pct":card.get("mem_util_pct"),"dcu_power_w":card.get("power_w"),"dcu_temp_c":card.get("temp_c"),
                      "dcu_temp_edge_c":card.get("temp_edge_c"),"dcu_temp_junction_c":card.get("temp_junction_c"),
                      "dcu_temp_mem_c":card.get("temp_mem_c"),"dcu_temp_core_c":card.get("temp_core_c"),"error":sample.get("error","")}
                dcu_rows[node].append(drow); dw.writerow(drow)
            handles[node][0].flush(); handles[node][1].flush()
            if len(host_rows[node])==1:
                card_count=len(sample.get("dcus",[])); expected_cards=int(cfg.get("expected_dcu_cards_per_node",4))
                print("[%s/%s] 首包: DCU=%d, CPU温度=%s, CPU功耗=%s, 整机功耗=%s"%(role,node,card_count,
                      "--" if sample.get("cpu_temp_avg_c") is None else "%.1fC"%sample["cpu_temp_avg_c"],
                      "--" if sample.get("cpu_power_w") is None else "%.1fW"%sample["cpu_power_w"],
                      "--" if sample.get("node_power_w") is None else "%.1fW"%sample["node_power_w"]),flush=True)
                if card_count!=expected_cards:print("[%s/%s] 警告: 期望 %d 张 DCU，实际采集到 %d 张"%(role,node,expected_cards,card_count),file=sys.stderr,flush=True)
            if not first_packet_check_printed and len(seen)==len(roles):
                cards_by_role=[]
                for group,group_nodes in active_groups.items():
                    cards_by_role.append("%s: %s"%(group,", ".join("%s(%s卡)"%(name,node_health[name].get("dcu_count","--")) for name in group_nodes)))
                all_base_ok=all(node_health[name].get("dcu_count")==int(cfg.get("expected_dcu_cards_per_node",4)) and not node_health[name].get("last_error") for name in roles)
                label="所有节点首包正常" if all_base_ok else "所有节点首包已收到，但存在采集异常"
                print("[节点检查] %s | %s"%(label,"; ".join(cards_by_role)),flush=True)
                first_packet_check_printed=True
    stop.set(); ended=time.time(); total_elapsed=round(ended-started,3)
    shared_steady=detect_steady_state(cfg,mode,active_groups,dcu_rows,total_elapsed); shared_steady["scope"]="shared"
    node_steady=detect_node_steady_states(cfg,roles,dcu_rows,total_elapsed)
    mark_phases(host_rows,dcu_rows,shared_steady,"phase"); mark_phases(host_rows,dcu_rows,shared_steady,"shared_phase")
    mark_node_phases(host_rows,dcu_rows,node_steady)
    shared_host={node:steady_rows(rows,shared_steady) for node,rows in host_rows.items()}
    shared_dcu={node:steady_rows(rows,shared_steady) for node,rows in dcu_rows.items()}
    node_host={node:steady_rows(rows,node_steady[node]) for node,rows in host_rows.items()}
    node_dcu={node:steady_rows(rows,node_steady[node]) for node,rows in dcu_rows.items()}
    full_scope={"scope":"full","status":"full_run","start_elapsed_s":0.0,"end_elapsed_s":total_elapsed}
    all_shared_summary=[]; all_node_summary=[]; all_full_summary=[]; node_infos=[]
    for node,role in roles.items():
        ndir=outdir/role/node
        write_csv(ndir/"host.csv",HOST_FIELDS,host_rows[node]); write_csv(ndir/"dcu_cards.csv",NODE_DCU_FIELDS,dcu_rows[node])
        shared_summary=build_node_summary(role,node,shared_host[node],shared_dcu[node])
        per_node_summary=build_node_summary(role,node,node_host[node],node_dcu[node])
        full_summary=build_node_summary(role,node,host_rows[node],dcu_rows[node])
        all_shared_summary.extend(shared_summary); all_node_summary.extend(per_node_summary); all_full_summary.extend(full_summary)
        # summary.csv 与 visualization.svg 继续代表统一口径，兼容已有分析流程。
        write_csv(ndir/"summary.csv",SUMMARY_FIELDS,shared_summary)
        write_csv(ndir/"shared_summary.csv",SUMMARY_FIELDS,shared_summary)
        write_csv(ndir/"per_node_summary.csv",SUMMARY_FIELDS,per_node_summary)
        write_csv(ndir/"full_summary.csv",SUMMARY_FIELDS,full_summary)
        make_node_svg(ndir/"visualization.svg",model,role,node,host_rows[node],dcu_rows[node],events,started,shared_steady)
        make_node_svg(ndir/"visualization_per_node.svg",model,role,node,host_rows[node],dcu_rows[node],events,started,node_steady[node])
        make_node_svg(ndir/"visualization_full.svg",model,role,node,host_rows[node],dcu_rows[node],events,started,full_scope)
        node_infos.append((role,node,"%s/%s/visualization_full.svg"%(role,node),"%s/%s/visualization.svg"%(role,node),"%s/%s/visualization_per_node.svg"%(role,node)))
    write_csv(outdir/"summary.csv",SUMMARY_FIELDS,all_shared_summary)
    write_csv(outdir/"shared_summary.csv",SUMMARY_FIELDS,all_shared_summary)
    write_csv(outdir/"per_node_summary.csv",SUMMARY_FIELDS,all_node_summary)
    write_csv(outdir/"full_summary.csv",SUMMARY_FIELDS,all_full_summary)
    make_dashboard(outdir/"dashboard.html",model,node_infos,host_rows,dcu_rows,shared_host,shared_dcu,node_host,node_dcu,shared_steady,node_steady)
    steady_report={**shared_steady,"shared":shared_steady,"per_node":node_steady}
    (outdir/"steady_state.json").write_text(json.dumps(steady_report,ensure_ascii=False,indent=2),encoding="utf-8")
    metadata={"model_name":model,"deployment_mode":mode,"started_at":iso_time(started),"ended_at":iso_time(ended),"duration_s":total_elapsed,
              "nodes":roles,"route_events":events,"steady_state":steady_report}
    (outdir/"run_metadata.json").write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding="utf-8")
    if shared_steady.get("start_elapsed_s") is not None:
        steady_text="%.1fs–%.1fs，持续%.1fs，参考组%s，结束%s"%(shared_steady["start_elapsed_s"],shared_steady["end_elapsed_s"],shared_steady["duration_s"],shared_steady.get("reference_group"),"已确认" if shared_steady.get("end_confirmed") else "未确认")
    else:steady_text="未检测到（稳态汇总为空，请查看 full_summary.csv）"
    node_detected=sum(1 for result in node_steady.values() if result.get("start_elapsed_s") is not None)
    print("\n监控结束，共 %.1fs。\n统一稳态区间: %s\n每节点独立稳态: 已检测 %d/%d 个节点\n统一口径汇总: %s\n独立口径汇总: %s\n全程汇总: %s\n可视化入口: %s"%(
          total_elapsed,steady_text,node_detected,len(node_steady),outdir/"shared_summary.csv",outdir/"per_node_summary.csv",outdir/"full_summary.csv",outdir/"dashboard.html"),flush=True)


if __name__=="__main__": main()
