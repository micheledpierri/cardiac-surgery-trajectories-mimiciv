#!/usr/bin/env python3
"""TRAJECTORIES Study 1 — Phase 03_08 reproducible support transitions.

Reads only frozen six-hour Parquets; does not access MIMIC or rerun extraction.
Final output: the three canonical CSV files in data/. No exploratory version chain.

Primary respiratory state (observation-aware):
  V1: invasive_ventilation_fraction > 0
  V0: fraction == 0 AND respiratory_classified_minutes > 0
  NA: not eligible, missing fraction, or no classified respiratory minutes
Partial coverage is retained and reported as an explicit limitation.
V denotes documented INVASIVE_VENT time, not extubation or reintubation.

A: vasoactive_any_exposed_flag, 0/1; missing values remain missing.
The original v3/v4 definition is recomputed internally for a blocking
regression check but is not saved as a second set of published outputs.

Run from the repository root:
  python Scripts/28_trajectory_transitions.py
Use --project to select a project containing frozen inputs in data/.
Use --output-dir to write validation outputs outside canonical data/.
The three CSVs in the selected output directory are replaced ONLY after all checks pass.
A temporary backup protects the previous files if replacement fails.
Requires: pandas, numpy, pyarrow.
"""
from __future__ import annotations
import argparse
import json
import math
import os
import shutil
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd

WINDOWS = ('H00-H06','H06-H12','H12-H18','H18-H24')
Z = 1.959963984540054
COHORT = 9305
TRANSITIONS = ['V0_A0','V0_A1','V1_A0','V1_A1']


def require(ok, message):
    if not ok:
        raise ValueError('03_08 validation failed: ' + message)


def wilson(k,n):
    if n == 0:
        return (np.nan,np.nan)
    p = k/n
    den=1+Z*Z/n
    mid=(p+Z*Z/(2*n))/den
    half=Z/den*math.sqrt(p*(1-p)/n+Z*Z/(4*n*n))
    return (round(100*max(0,mid-half),3),round(100*min(1,mid+half),3))


def fraction(k,n):
    return round(100*k/n,3) if n else np.nan


def normalize(s):
    return s.astype(str).str.replace('_','-',regex=False).str.strip()


def check_source(d,name):
    require(len(d)==4*COHORT,f'{name}: expected {4*COHORT} rows, got {len(d)}')
    require(d.stay_id.nunique()==COHORT, f'{name}: stay count differs from {COHORT}')
    require(not d.duplicated(['stay_id','window_label']).any(), f'{name}: duplicate stay-window')
    require(set(d.window_label)==set(WINDOWS), f'{name}: unexpected window labels')
    require((d.groupby('stay_id').window_label.nunique()==4).all(), f'{name}: incomplete master grid')


def joint_table(v,a):
    rows=[]
    for earlier,later in zip(WINDOWS[:-1],WINDOWS[1:]):
        valid=v[earlier].notna()&v[later].notna()&a[earlier].notna()&a[later].notna()
        nall=int(valid.sum())
        c0='V'+v[earlier].fillna(-1).astype(int).astype(str)+'_A'+a[earlier].fillna(-1).astype(int).astype(str)
        c1='V'+v[later].fillna(-1).astype(int).astype(str)+'_A'+a[later].fillna(-1).astype(int).astype(str)
        for initial in TRANSITIONS:
            denom=int((valid&(c0==initial)).sum())
            for next_state in TRANSITIONS:
                count=int((valid&(c0==initial)&(c1==next_state)).sum())
                lo,hi=wilson(count,nall)
                clo,chi=wilson(count,denom)
                rows.append(dict(from_window=earlier,to_window=later,initial_state=initial,
                   next_state=next_state,n_transition=count,n_all_paired=nall,
                   pct_all_paired=fraction(count,nall),ci95_all_paired_low_pct=lo,
                   ci95_all_paired_high_pct=hi,n_initial_state=denom,
                   pct_conditional_on_initial=fraction(count,denom),
                   ci95_conditional_low_pct=clo,ci95_conditional_high_pct=chi))
        require(sum(r['n_transition'] for r in rows if r['from_window']==earlier)==nall,
                'joint sum mismatch '+earlier)
    return pd.DataFrame(rows)


def sequence_table(v,a):
    rows=[]
    for domain,x in [('ventilation_any_in_window',v),('vasoactive_any_in_window',a)]:
        complete=x.notna().all(axis=1)
        d=int(complete.sum())
        patterns=x.loc[complete,list(WINDOWS)].astype(int).astype(str).agg(''.join,axis=1).value_counts()
        for j in range(16):
            pattern=f'{j:04b}'
            k=int(patterns.get(pattern,0))
            lo,hi=wilson(k,d)
            rows.append(dict(domain=domain,sequence=pattern,n=k,denominator=d,pct=fraction(k,d),
                   wilson95_low_pct=lo,wilson95_high_pct=hi))
        require(sum(r['n'] for r in rows if r['domain']==domain)==d,domain+' sequence sum mismatch')
    return pd.DataFrame(rows)


def directions(delta):
    return pd.Series(np.select([delta<0,delta>0,delta==0],
             ['decrease','increase','unchanged'],default='missing'),index=delta.index).where(delta.notna(),'missing')


def directional_table(d):
    columns={'vent_fraction':'vent_fraction_observed','nee_twa':'nee_twa',
       'fluid_cumulative_level':'fluid_accumulation_cumulative_pct_weight','lactate_median':'lactate_median_mmol_l'}
    piv={name:d.pivot(index='stay_id',columns='window_label',values=field).reindex(columns=list(WINDOWS))
         for name,field in columns.items()}
    pairs=[('vent_fraction','nee_twa'),('vent_fraction','fluid_cumulative_level'),
           ('nee_twa','fluid_cumulative_level'),('vent_fraction','lactate_median'),
           ('nee_twa','lactate_median')]
    rows=[]
    for wa,wb in zip(WINDOWS[:-1],WINDOWS[1:]):
        delta={key:x[wb]-x[wa] for key,x in piv.items()}
        for m1,m2 in pairs:
            ok=delta[m1].notna()&delta[m2].notna()
            dd1=directions(delta[m1]);dd2=directions(delta[m2]);den=int(ok.sum())
            k=int((ok&(dd1=='decrease')&(dd2=='decrease')).sum())
            lo,hi=wilson(k,den)
            rows.append(dict(from_window=wa,to_window=wb,measure_1=m1,measure_2=m2,
               both_decrease_n=k,jointly_observed_n=den,both_decrease_pct=fraction(k,den),
               wilson95_low_pct=lo,wilson95_high_pct=hi,
               interpretation='joint direction prevalence; not synchronization, causality or prediction'))
    return pd.DataFrame(rows)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--project',type=Path,default=Path(__file__).resolve().parent.parent if Path(__file__).parent.name.lower()=='scripts' else Path(__file__).resolve().parent)
    p.add_argument('--output-dir',type=Path,default=None,
                   help='Output directory; defaults to <project>/data. Relative paths resolve from the current working directory.')
    args=p.parse_args()
    root=args.project.resolve(); out=args.output_dir.resolve() if args.output_dir is not None else root/'data'
    src=root/'data'/'02_01_study1_six_hour.parquet'
    resp=root/'data'/'01_11_respiratory_support_six_hour.parquet'
    require(src.is_file() and resp.is_file(),f'missing frozen input(s): {src}, {resp}')
    cols=['stay_id','window_label','invasive_ventilation_fraction','vasoactive_any_exposed_flag',
          'nee_twa','fluid_accumulation_cumulative_pct_weight','lactate_median_mmol_l']
    d=pd.read_parquet(src,columns=cols)
    r=pd.read_parquet(resp,columns=['stay_id','window_label','eligible_minutes','respiratory_classified_minutes'])
    for x in (d,r):x['window_label']=normalize(x.window_label)
    check_source(d,'integrated');check_source(r,'respiratory')
    d=d.merge(r,on=['stay_id','window_label'],how='left',validate='one_to_one',indicator=True)
    require((d['_merge']=='both').all(),'respiratory join missing');d=d.drop(columns='_merge')
    for field in cols[2:]+['eligible_minutes','respiratory_classified_minutes']:
        d[field]=pd.to_numeric(d[field],errors='raise')
    require((d.eligible_minutes>=0).all(),'negative eligible minutes')
    require((d.respiratory_classified_minutes>=0).all(),'negative classified minutes')
    require((d.respiratory_classified_minutes<=d.eligible_minutes+1e-5).all(),'classified > eligible')
    fraction_v=d.invasive_ventilation_fraction
    require(not ((fraction_v.notna())&((fraction_v<0)|(fraction_v>1))).any(),'invalid invasive fractions')
    a=d.vasoactive_any_exposed_flag
    require(set(a.dropna().unique()).issubset({0,1}),'nonbinary vasoactive exposure')
    legacy=np.where(fraction_v.isna(),np.nan,(fraction_v>0).astype(float))
    unknown=(d.eligible_minutes>0)&(d.respiratory_classified_minutes==0)
    observed=np.where((d.eligible_minutes<=0)|unknown|fraction_v.isna(),np.nan,(fraction_v>0).astype(float))
    d['v_legacy']=legacy;d['v_observed']=observed
    # The same observability rule must apply to directional ventilation changes.
    d['vent_fraction_observed']=fraction_v.where(~((d.eligible_minutes<=0)|unknown),np.nan)
    pv=lambda col:d.pivot(index='stay_id',columns='window_label',values=col).reindex(columns=list(WINDOWS))
    aa=pv('vasoactive_any_exposed_flag');vl=pv('v_legacy');vo=pv('v_observed')
    old_j=joint_table(vl,aa);new_j=joint_table(vo,aa)
    old_s=sequence_table(vl,aa);new_s=sequence_table(vo,aa)
    direct=directional_table(d)
    # Blocking regression checks against the already reconciled v3/v4 output.
    old_den=[int(x) for x in old_j.groupby('from_window',sort=False).n_all_paired.first()]
    new_den=[int(x) for x in new_j.groupby('from_window',sort=False).n_all_paired.first()]
    require(old_den==[9301,9299,9237],f'legacy denominators changed: {old_den}')
    require(new_den==[9287,9193,9027],f'observation-aware denominators unexpected: {new_den}')
    old_vent=int(old_s.loc[old_s.domain=='ventilation_any_in_window','denominator'].iloc[0])
    new_vent=int(new_s.loc[new_s.domain=='ventilation_any_in_window','denominator'].iloc[0])
    old_vaso=int(old_s.loc[old_s.domain=='vasoactive_any_in_window','denominator'].iloc[0])
    new_vaso=int(new_s.loc[new_s.domain=='vasoactive_any_in_window','denominator'].iloc[0])
    require((old_vent,new_vent,old_vaso,new_vaso)==(9237,9018,9237,9237),
        'four-window denominators changed')
    require(int(unknown.sum())==257,f'number of entirely unclassified eligible windows changed: {int(unknown.sum())}')
    # Check all individual source rows belong to exactly one of three respiratory categories.
    require(int(np.isfinite(observed).sum()) + int(np.isnan(observed).sum())==len(d),
        'respiratory partition failure')
    require(len(new_j)==48 and len(new_s)==32 and len(direct)==15,'unexpected output table dimensions')
    require(all(new_j.groupby('from_window',sort=False).n_transition.sum().values==np.array(new_den)),
        'joint transition totals inconsistent')
    files={
      '03_08_joint_transitions_wilson95.csv':new_j,
      '03_08_support_sequences_wilson95.csv':new_s,
      '03_08_directional_summary_wilson95.csv':direct
    }
    # Stage everything first; do not delete or overwrite published CSVs on a failed run.
    out.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.03_08_stage_',dir=out) as tmp_name:
        stage=Path(tmp_name)
        for name,frame in files.items():
            frame.to_csv(stage/name,index=False,float_format='%.3f')
            check=pd.read_csv(stage/name)
            require(len(check)==len(frame) and list(check.columns)==list(frame.columns),
                f'output round-trip failed: {name}')
        backed_up=[]
        promoted=[]
        try:
            for name in files:
                target=out/name
                if target.exists():
                    shutil.copy2(target,stage/(name+'.backup'))
                    backed_up.append(name)
            for name in files:
                os.replace(stage/name,out/name)
                promoted.append(name)
        except Exception:
            for name in reversed(promoted):
                target=out/name
                if name in backed_up:
                    os.replace(stage/(name+'.backup'),target)
                elif target.exists():
                    target.unlink()
            raise
    print('PASS: 03_08 canonical outputs replaced in',out)
    for name,frame in files.items():
        print(f'  {name}: {len(frame)} rows')
    print('Eligible windows with zero respiratory classified time:',int(unknown.sum()))
    print('Original paired denominators:',old_den)
    print('Canonical paired denominators:',new_den)
    print('Original/canonical full-window ventilation sequences:',old_vent,'/',new_vent)
    print('Vasoactive full-window sequences:',new_vaso)
    print('Source Parquet files have not been modified.')

if __name__=='__main__':main()
