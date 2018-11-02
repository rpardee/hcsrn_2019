/*********************************************
* Roy Pardee
* Group Health Research Institute
* (206) 287-2078
* pardee.r@ghc.org
*
* C:\Users/pardre1/Documents/vdw/lhs/programs/hospitalizations/get_features.sas
*
* Grabs the features we want to use to predict hospitalizations for
* ppl with diabetes, chf or esrd.
*********************************************/

*  Date parameters ;
%global acg_mo
        hsp_st
        hsp_en
        feat_start
        feat_end
        ;

%macro pop_dates(index_date) ;
  /*
    its worth fussing over the date ranges here so we are sure they are correct.
    from the acg docs on xwiki:

      An example of the dates of the file names, enrollment, and service dates used
      in calculation: the ACG file with the date of May 2015 will include all people
      who were enrolled at some point in the month of May 2015.  The ACG calculated
      measures will include all services (px/dx/rx) in the time span of June 1, 2014
      through May 31, 2015.

    So the files are based on all services in the 12 mos ending in the file name.
    The IP risk prediction period starts on the following month.

    index_date here gives the start of the hospitalization prediction window.
    so we back it up by 1 month to get the acg month.
    we want 3 mos worth of features, that period ending in acg month

    so, if index is may 2016:
      we want acg_201604
      count hosps between may 2016 and april 2017
      use features between feb and april 2016

    what we have right now:
      Pulling ACG preds/comorbs from the file for 201604 (april 2016) CORRECT
      Looking at actual hospitalizations from 01JUN2016 through 31MAY2017 WRONG: should be may 2016 through apr 2017
      Pulling features from the period from 01MAR2016 through 31MAY2016 WRONG: should be feb 2016 through april 2016

    after fix:
      Pulling ACG preds/comorbs from the file for 201604 (april 2016) CORRECT
      Looking at actual hospitalizations from 01MAY2016 through 30APR2017 CORRECT
      Pulling features from the period from 01FEB2016 through 30APR2016 CORRECT


  */

  %let acg_mo     = %sysfunc(intnx(month, "&index_date"d,  -1     ), yymmn6.) ;
  %let hsp_st     = %sysfunc(intnx(month, "&index_date"d,   0, beg),  date9.) ;
  %let hsp_en     = %sysfunc(intnx(month, "&index_date"d, +11, end),  date9.) ;

  %let feat_start = %sysfunc(intnx(month, "&index_date"d,  -3     ),  date9.) ;
  %let feat_end   = %sysfunc(intnx(month, "&index_date"d,  -1, end),  date9.) ;

  %put Pulling ACG preds/comorbs from the file for &acg_mo ;
  %put Looking at actual hospitalizations from &hsp_st through &hsp_en ;
  %put Pulling features from the period from &feat_start through &feat_end ;
%mend pop_dates ;


%macro get_pop(outset = cohort) ;
  %* DRGs signifying pregnancy or accidents--IPs we dont want to try and predict ;
  %let preg_acc = '082', '083', '084', '085', '086', '087', '089', '131', '155', '183', '184',
                  '266', '463', '464', '465', '492', '511', '533', '534', '536', '537', '562',
                  '563', '604', '605', '765', '766', '767', '768', '774', '775', '776', '781',
                  '782', '789', '792', '793', '794', '795', '857', '862', '863', '902', '903',
                  '908', '923', '928', '933', '935', '955', '956', '957', '963', '965'
                  ;

  libname acg "&ghridw_root/sasdata/acg" ;
  libname cmd "&ghridw_root/sasdata/cmd" ;

  data ppls ;
    length chsid $ 10 esrd diabetes chf 3 ;
    set acg.acg_&acg_mo (keep = chsid dialysis_service chronic_renal_failure_condition congestive_heart_failure_conditi diabetes_condition probability_ip_hosp) ;

    esrd     = (chronic_renal_failure_condition ne 'NP' OR dialysis_service = '1') ;
    diabetes = (diabetes_condition ne 'NP') ;
    chf      = (congestive_heart_failure_conditi ne 'NP') ;

    mrn = chsid ;
    combo = '___' ;
    if esrd     then substr(combo, 1, 1) = 'E' ;
    if diabetes then substr(combo, 2, 1) = 'D' ;
    if chf      then substr(combo, 3, 1) = 'C' ;
    label
      combo = "Combination of ACG-detected conditions: [E]SRD, [D]iabetes & [C]HF"
      esrd = "CKD + Dialysis"
      chf = "Congestive Heart Failure"
    ;
    if esrd or diabetes or chf ;
    drop chsid ;
  run ;

  proc sql noprint feedback ;
    create table with_hosp_info as
    select p.*
        , (not i.chsid is null) as was_hospitalized "Hospitalized between &hsp_st and &hsp_en.."
        , drgkey
        , admitdt
        , msdrg
        , catx(': ', msdrg, ms_drg_desc) as ms_drg length = 60
        , uniform(7817) as randy
    from ppls as p LEFT JOIN
          cmd.ip_summary as i
    on    p.mrn = i.chsid AND
          i.admitdt between dhms("&hsp_st"d, 0, 0, 0) and dhms("&hsp_en"d, 23, 59, 59) /* AND
          msdrg not in (&preg_acc) */
    /* order by (not i.chsid is null), diabetes, esrd, chf */
    ;
  quit ;

  data with_hosp_info s.nonprev_hosps ;
    set with_hosp_info ;
    if msdrg in (&preg_acc) then output s.nonprev_hosps ;
    else output with_hosp_info ;
  run ;

  * Anybody w/> 1 hospitalization will appear multiple times--whittle down to a single rec/person ;
  proc sort data = with_hosp_info ;
    by mrn admitdt ;
  run ;

  data with_hosp_info ;
    set with_hosp_info ;
    by mrn ;
    if last.mrn ;
  run ;

  proc sort data = with_hosp_info ;
    by was_hospitalized diabetes esrd chf ;
  run ;


  * We want an 80/20 split on dev/validation, stratified on was_hospitalized ;
  proc surveyselect
    data     = with_hosp_info
    out      = with_selections
    method   = srs
    samprate = .8
    seed     = 890
    outall
  ;
    strata was_hospitalized diabetes esrd chf ;
  run ;

  data &outset(label = "Comorbs/preds from acg_&acg_mo.; IP counts between &hsp_st and &hsp_en") ;
    set with_selections ;
    if selected then portion = 'development' ;
    else portion = 'validation' ;
    drop selected ;
  run ;

  proc sort nodupkey data = &outset ;
    by mrn ;
  run ;
%mend get_pop ;

%macro get_features(inpop = cohort, outset = s.features) ;
  %let td_goo = user              = "&username@LDAP"
                password          = "&password"
                server            = "&td_prod"
                schema            = "%sysget(username)"
                connection        = global
                mode              = teradata
                fastload          = yes
  ;


  libname td teradata &td_goo multi_datasrc_opt = in_clause ;
  %removedset(dset = &outset) ;
  %removedset(dset = td.feat_cohort) ;

  %let dontwant = dontwant ;
  proc format ;
    value agecat
      00 - 04 = '00to04'
      05 - 09 = '05to09'
      10 - 14 = '10to14'
      15 - 19 = '15to19'
      20 - 29 = '20to29'
      30 - 39 = '30to39'
      40 - 49 = '40to49'
      50 - 59 = '50to59'
      60 - 64 = '60to64'
      65 - 69 = '65to69'
      70 - 74 = '70to74'
      75 - 79 = '75to79'
      80 - 84 = '80to84'
      85 - high = '85+'
      other = "&dontwant"
    ;
  quit ;

  data td.feat_cohort ;
    length mrn $ 10 ;
    set &inpop ;
  run ;

  %let fc_len = 12 ;
  %let ft_len = 9 ;
  %let fr_len = 8 ;
  %let fd_len = 4 format = mmddyy10. ;

  proc sql noprint ;
    create table grist as
    select c.mrn, gender, birth_date, put(%calcage(bdtvar = birth_date, refdate = "&feat_end"d), agecat.) as age_category
    from td.feat_cohort as c INNER JOIN
          &_vdw_demographic as d
    on    c.mrn = d.mrn
    where CALCULATED age_category ne "&dontwant"
    ;

    create table demog_features as
    select mrn
        , birth_date as feature_date length = &fd_len
        , 'agegender' as feature_type length = &ft_len
        , catx('_', age_category, gender) as feature_code length = &fc_len
        , ' ' as feature_result length = &fr_len
    from grist
    ;
    drop table grist ;
  quit ;

  proc append base = &outset data = demog_features ;
  run ;

  libname vdw "&ghridw_root/sasdata/crn_vdw" ;

  proc sql noprint ;
    drop table demog_features ;

    create table rx_features as
    select c.mrn
        , rxdate as feature_date length = &fd_len
        , 'rx' as feature_type length = &ft_len
        , ndc as feature_code length = &fc_len
        , ' ' as feature_result length = &fr_len
    from td.feat_cohort as c INNER JOIN
        &_vdw_rx as r
    on    c.mrn = r.mrn AND
          rxdate between "&feat_start"d and "&feat_end"d
    ;

    create table rx_with_cuis as
    select r.mrn
          , r.feature_type
          , coalesce(u.rxn_rxcui, r.feature_code) as feature_code length = &fc_len
          , r.feature_date
          , r.feature_result
    from  rx_features as r LEFT JOIN
          vdw.unifiedndc as u
    on    r.feature_code = u.ndc
    ;

  quit ;

  proc append base = &outset data = rx_with_cuis ;
  run ;

  proc sql noprint ;
    drop table rx_features ;

    create table lab_features as
    select c.mrn
        , lab_dt as feature_date length = &fd_len
        , 'lab' as feature_type length = &ft_len
        , catx(':', test_type, abn_ind) as feature_code length = &fc_len
        , result_c as feature_result length = &fr_len
    from td.feat_cohort as c INNER JOIN
        &_vdw_lab as r
    on    c.mrn = r.mrn AND
          lab_dt between "&feat_start"d and "&feat_end"d
    ;

  quit ;

  proc append base = &outset data = lab_features ;
  run ;

  proc sql noprint ;
    drop table lab_features ;

    create table dx_features as
    select c.mrn
        , ghc_diagdate as feature_date length = &fd_len
        , 'dx' as feature_type length = &ft_len
        , dx as feature_code length = &fc_len
        , ' ' as feature_result length = &fr_len
    from td.feat_cohort as c INNER JOIN
        &_vdw_dx as r
    on    c.mrn = r.mrn AND
          ghc_diagdate between "&feat_start"d and "&feat_end"d
    ;

  quit ;

  proc append base = &outset data = dx_features ;
  run ;

  proc sql noprint ;
    drop table dx_features ;

    create table px_features as
    select c.mrn
        , procdate as feature_date   length = &fd_len
        , 'px'     as feature_type   length = &ft_len
        , px       as feature_code   length = &fc_len
        , cptmod1  as feature_result length = &fr_len
    from td.feat_cohort as c INNER JOIN
        &_vdw_px as r
    on    c.mrn = r.mrn AND
          procdate between "&feat_start"d and "&feat_end"d
    ;

  quit ;

  proc append base = &outset data = px_features ;
  run ;

  proc format ;
    value bmicat
      low  -< 18.5 = 'und'
      18.5 -< 25   = 'nrm'
      25   -< 30   = 'ovw'
      30   -< 35   = 'ob1'
      35   -< 40   = 'ob2'
      40   -< 45   = 'ob3'
      45   -< 50   = 'ob4'
      50   -< 55   = 'ob5'
      55   - high  = 'ob6'
    ;
  quit ;


  proc sql noprint ;
    drop table px_features ;
    * weight (lb) / [height (in)]^2 x 703 ;
    create table bmi_features as
    select c.mrn
        , measure_date as feature_date length = &fd_len
        , 'bmi'        as feature_type length = &ft_len
        , put((wt/(ht**2)) * 703, bmicat.) as feature_code length = &fc_len
        , put((wt/(ht**2)) * 703, 8.2) as feature_result length = &fr_len
    from td.feat_cohort as c INNER JOIN
        &_vdw_vitalsigns as v
    on    c.mrn = v.mrn AND
          measure_date between "&feat_start"d and "&feat_end"d
    where ht is not null and wt is not null
    ;

  quit ;

  proc append base = &outset data = bmi_features ;
  run ;

  proc format ;
    value syscat
      low -< 120 = 'norm'
      120 -< 130 = 'elev'
      130 - high = 'high'
    ;
  quit ;

  proc sql noprint ;
    * drop table bmi_features ;

    * weight (lb) / [height (in)]^2 x 703 ;
    create table bp_features as
    select c.mrn
        , measure_date as feature_date length = &fd_len
        , 'bp'        as feature_type length = &ft_len
        , case
            when diastolic gt 80 then 'high'
            else put(systolic, syscat.)
          end as feature_code length = &fc_len
        , catx('/', systolic, diastolic) as feature_result length = &fr_len
    from td.feat_cohort as c INNER JOIN
        &_vdw_vitalsigns as v
    on    c.mrn = v.mrn AND
          measure_date between "&feat_start"d and "&feat_end"d
    where systolic is not null and diastolic is not null
    ;

  quit ;

  proc append base = &outset data = bp_features ;
  run ;
%mend get_features ;

