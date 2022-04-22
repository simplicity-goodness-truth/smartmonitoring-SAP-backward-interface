FUNCTION zanomaly_data_collector.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(SELOPT_PARA) TYPE  /SDF/E2E_SELECTION_PARA_T
*"  EXPORTING
*"     VALUE(RESULT) TYPE  /SDF/E2E_RESULT_TT
*"     VALUE(RETURN_STATUS) TYPE  /SDF/E2E_EFWKE_RETURN_STATUS_T
*"----------------------------------------------------------------------


  DATA:
    ls_parameter TYPE /sdf/e2e_para,
    ls_result    TYPE /sdf/e2e_result.

  DATA: t_result              TYPE /sdf/e2e_result_tt.
  DATA: l_result              LIKE LINE OF t_result.

  DATA: l_parameter           TYPE /sdf/e2e_para.
  DATA: t_parameter           TYPE /sdf/e2e_para_tt.

  DATA: t_return_status       LIKE return_status.
  DATA: l_return_status       LIKE LINE OF t_return_status.
  DATA: wa_return_status      LIKE LINE OF return_status.

  FIELD-SYMBOLS <ls_selopt_para> TYPE /sdf/e2e_selection_para.
  FIELD-SYMBOLS: <selection_parameter> TYPE /sdf/e2e_selection_params.

  FIELD-SYMBOLS <selection_ranges> TYPE /sdf/ranges.
  FIELD-SYMBOLS <context_ranges> TYPE /sdf/ranges.
  FIELD-SYMBOLS <metric_ranges> TYPE /sdf/ranges.

  DATA lo_sys_exception TYPE REF TO cx_ai_system_fault.
  DATA lv_response_string TYPE string.


  DATA lt_abnormal_pair TYPE zcl_intelligent_alerting=>tt_abnormal_pair.
  DATA lv_abnormal_pair TYPE string.

  DATA lv_time TYPE uzeit.
  DATA lv_date TYPE datum.
  DATA lv_timezone TYPE timezone.

  DATA lt_mode TYPE /sdf/e2e_selop_tt.

  DATA lv_utc_tstmp TYPE timestamp.
  DATA lv_date_char TYPE char10.
  DATA lv_abnormality_char TYPE char10.

  DATA lv_empty_delta TYPE boolean.


  " Structures for logging

  DATA:
    BEGIN OF ls_string,
      part1 TYPE symsgv,
      part2 TYPE symsgv,
      part3 TYPE symsgv,
      part4 TYPE symsgv,
    END OF ls_string.

  CLEAR t_result.
  CLEAR t_parameter.
  CLEAR t_return_status.

  REFRESH result.
  REFRESH return_status.


  LOOP AT selopt_para ASSIGNING <ls_selopt_para>.

    LOOP AT <ls_selopt_para>-selection_parameter ASSIGNING <selection_parameter>.


      CASE <selection_parameter>-param.

        WHEN 'PAIR'.
          lt_mode = <selection_parameter>-t_ranges.

      ENDCASE.

      LOOP AT <selection_parameter>-t_ranges ASSIGNING <selection_ranges>.

        l_parameter-param = <selection_parameter>-param.
        l_parameter-value = <selection_ranges>-low.

        APPEND l_parameter TO t_parameter.
        l_result-result-t_parameter = t_parameter.

      ENDLOOP. "  LOOP AT <selection_parameter>-t_ranges ASSIGNING <selection_ranges>.

    ENDLOOP. " LOOP AT <ls_selopt_para>-selection_parameter ASSIGNING <selection_parameter>.

  ENDLOOP. " LOOP AT selopt_para ASSIGNING <ls_selopt_para>.

  " Converting system time and date to UTC timestamp

  CONVERT DATE sy-datum TIME sy-uzeit INTO TIME STAMP lv_utc_tstmp TIME ZONE sy-zonlo.

  " Splitting timestamp into date and time in UTC

  CONVERT TIME STAMP lv_utc_tstmp TIME ZONE 'UTC' INTO DATE lv_date TIME lv_time.

  " Taking a 6 minutes span back for abnormality check

  lv_time = lv_time - 360.
*
  zcl_intelligent_alerting=>import_anomalies_by_timestamp(
    EXPORTING
       ip_time          = lv_time
       ip_date          = lv_date
    IMPORTING
        et_abnormal_pair = lt_abnormal_pair ).

  IF ( lt_abnormal_pair IS NOT INITIAL ).

    CLEAR l_result.

    CALL FUNCTION 'GET_SYSTEM_TIMEZONE'
      IMPORTING
        timezone = lv_timezone.

    LOOP AT lt_abnormal_pair ASSIGNING FIELD-SYMBOL(<ls_abnormal_pair>).

      REFRESH t_parameter.

      " Converting YYYYMMDD to DD.MM.YYYY

      CONCATENATE <ls_abnormal_pair>-anomaly_date+6(2) <ls_abnormal_pair>-anomaly_date+4(2) <ls_abnormal_pair>-anomaly_date(4) INTO lv_date_char  SEPARATED BY '.'.

      " Preparing line with object and metric

      CONCATENATE <ls_abnormal_pair>-metric_short_text 'on' <ls_abnormal_pair>-context_name  INTO lv_abnormal_pair SEPARATED BY space.

      " Preparing abnormality rating

      lv_abnormality_char = substring_after( val = <ls_abnormal_pair>-abnormality_rating sub = '.' ).

      CONCATENATE lv_abnormality_char(2) '.' lv_abnormality_char+2 INTO lv_abnormality_char.

      l_parameter-param = 'PAIR'.
      l_parameter-value = lv_abnormal_pair.
      APPEND l_parameter TO t_parameter.
      l_result-result-t_parameter = t_parameter.

      l_result-result-text = lv_abnormality_char.
      CONDENSE l_result-result-text.

      l_result-result-count = 1.
      l_result-result-average =
      l_result-result-min =
      l_result-result-max =
      l_result-result-sum = lv_abnormality_char.

      l_result-call_id = <ls_selopt_para>-call_id.

      APPEND l_result TO t_result.

    ENDLOOP. " LOOP AT lt_abnormal_pair ASSIGNING FIELD-SYMBOL(<ls_abnormal_pair>).

  ELSE.

    REFRESH t_parameter.
    CLEAR l_result.

    l_parameter-param = 'PAIR'.
    l_parameter-value = 'No abnormal patterns detected'.
    APPEND l_parameter TO t_parameter.
    l_result-result-t_parameter = t_parameter.

    l_result-result-text = '0'.
    CONDENSE l_result-result-text.

    l_result-result-count = 1.
    l_result-result-average =
    l_result-result-min =
    l_result-result-max =
    l_result-result-sum = '0'.

    l_result-call_id = <ls_selopt_para>-call_id.
    APPEND l_result TO t_result.

  ENDIF. "IF ( lt_abnormal_pair IS NOt INITIAL ).

  wa_return_status-call_id = <ls_selopt_para>-call_id.
  wa_return_status-status = 0.
  wa_return_status-msgtext = 'Anomalies collector executed'.
  APPEND wa_return_status TO t_return_status.

  APPEND LINES OF t_result        TO result.
  APPEND LINES OF t_return_status TO return_status.

ENDFUNCTION.