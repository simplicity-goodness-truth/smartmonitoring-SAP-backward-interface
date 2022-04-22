FUNCTION zanomaly_stat_collector .
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     REFERENCE(SELOPT_PARA) TYPE  /SDF/E2E_SELECTION_PARA_T
*"  EXPORTING
*"     REFERENCE(RESULT) TYPE  /SDF/E2E_RESULT_TT
*"     REFERENCE(RETURN_STATUS) TYPE  /SDF/E2E_EFWKE_RETURN_STATUS_T
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
  FIELD-SYMBOLS <ls_sel_opt> TYPE /sdf/ranges.

  DATA lo_sys_exception TYPE REF TO cx_ai_system_fault.


  TYPES: BEGIN OF ty_sel_opt_sid_tt,
           sid TYPE char4,
         END OF ty_sel_opt_sid_tt.

  DATA lt_sel_opt_sid TYPE STANDARD TABLE OF ty_sel_opt_sid_tt.
  DATA ls_sel_opt_sid TYPE ty_sel_opt_sid_tt.


  DATA lv_events_count TYPE i.
  DATA lv_events_count_char TYPE char10.

  DATA lv_sid_regexp TYPE char20.

  DATA lv_call_id_char TYPE char4.

  DATA lv_key_fig TYPE char20.

  DATA lv_sum_duration TYPE integer.

  DATA lv_avg_duration TYPE p LENGTH 6 DECIMALS 2.
  DATA lv_avg_duration_char TYPE char10.

  TYPES: BEGIN OF ty_abap_instances_tt,
           name            TYPE char100,
           anomalies_count TYPE integer,
         END OF ty_abap_instances_tt.

  DATA lt_abap_instances TYPE STANDARD TABLE OF ty_abap_instances_tt.
  DATA ls_abap_instances TYPE ty_abap_instances_tt.

  DATA lv_max_anomalies TYPE integer.
  DATA lv_sum_anomalies TYPE integer.
  DATA lv_multiplier TYPE p LENGTH 4 DECIMALS 2.

  DATA lv_impact_counter TYPE integer.
  DATA lv_records_counter TYPE integer.
  DATA lv_balance_rating TYPE p LENGTH 4 DECIMALS 2.

  DATA lv_balance_rating_char TYPE char10.

  FIELD-SYMBOLS <ls_abap_instances> LIKE LINE OF lt_abap_instances.

  CLEAR t_result.
  CLEAR t_parameter.
  CLEAR t_return_status.
  CLEAR result.

  DATA lt_contexthier TYPE actcontexthierdir.
  FIELD-SYMBOLS <ls_contexthier> LIKE LINE OF lt_contexthier.
  DATA: lr_sys_context_names TYPE RANGE OF accontexthierdir-subcontext_name,
        wa_sys_context_names LIKE LINE OF lr_sys_context_names.

  " Preparing input parameters

  LOOP AT selopt_para ASSIGNING <ls_selopt_para>.

    LOOP AT <ls_selopt_para>-selection_parameter ASSIGNING <selection_parameter>.

      CASE <selection_parameter>-param.

        WHEN 'SYS_ID'.

          LOOP AT <selection_parameter>-t_ranges ASSIGNING FIELD-SYMBOL(<t_range>).

            IF ( <t_range>-sign ='I' AND <t_range>-option ='EQ').
              ls_sel_opt_sid-sid = <t_range>-low.
              APPEND ls_sel_opt_sid TO lt_sel_opt_sid.
            ENDIF.

          ENDLOOP." LOOP AT <selection_parameter>-t_ranges ASSIGNING FIELD-SYMBOL(<t_range>).

        WHEN 'KEY_FIG'.

          LOOP AT <selection_parameter>-t_ranges ASSIGNING FIELD-SYMBOL(<key_fig_range>).

            IF ( <key_fig_range>-sign ='I' AND <key_fig_range>-option ='EQ').
              lv_key_fig = <key_fig_range>-low.
            ENDIF.

          ENDLOOP." LOOP AT <selection_parameter>-t_ranges ASSIGNING FIELD-SYMBOL(<t_range>).

      ENDCASE. " CASE <selection_parameter>-param

    ENDLOOP. " LOOP AT <ls_selopt_para>-selection_parameter ASSIGNING <selection_parameter>.

  ENDLOOP. " LOOP AT selopt_para ASSIGNING <ls_selopt_para>.

  lv_events_count = 0.

  LOOP AT lt_sel_opt_sid ASSIGNING FIELD-SYMBOL(<ls_sel_opt_sid>).

    " Preparing a list of objects for the system

    CONCATENATE <ls_sel_opt_sid>-sid '%' INTO lv_sid_regexp.

    SELECT subcontext_name FROM accontexthierdir INTO CORRESPONDING FIELDS OF TABLE lt_contexthier
     WHERE context_name LIKE lv_sid_regexp
     AND ac_variant = 'A'.

    LOOP AT lt_contexthier ASSIGNING <ls_contexthier>.

      wa_sys_context_names-sign = 'I'.
      wa_sys_context_names-option = 'EQ'.
      wa_sys_context_names-low = <ls_contexthier>-subcontext_name.

      APPEND wa_sys_context_names TO lr_sys_context_names.

    ENDLOOP. "  LOOP AT lt_contexthier ASSIGNING <ls_contexthier>

    DELETE ADJACENT DUPLICATES FROM lr_sys_context_names COMPARING low .

    IF lr_sys_context_names IS NOT INITIAL.

      REFRESH t_parameter.

      CASE lv_key_fig.

          " Count amount of active anomalies in the moment

        WHEN  'OP_AN_CNT' .

          lv_events_count = 0.

          SELECT COUNT(*) INTO lv_events_count FROM zsmartmonitorlog
            WHERE event_date = sy-datum
            AND context_name IN lr_sys_context_names
            AND source = 'A'
            AND suppressed <> 'X'.

          IF ( sy-subrc <> 0 ).
            lv_events_count = 0.
          ENDIF.

          lv_events_count_char = lv_events_count.

          l_parameter-param = 'SYS_ID'.
          l_parameter-value = <ls_sel_opt_sid>-sid.
          APPEND l_parameter TO t_parameter.
          l_result-result-t_parameter = t_parameter.

          l_result-result-text = lv_events_count_char.
          CONDENSE l_result-result-text.

          l_result-result-count = 1.
          l_result-result-average =
          l_result-result-min =
          l_result-result-max =
          l_result-result-sum = lv_events_count_char.

          l_result-call_id = <ls_selopt_para>-call_id.

          APPEND l_result TO t_result.

          wa_return_status-call_id = <ls_selopt_para>-call_id.
          wa_return_status-status = 0.
          wa_return_status-msgtext = 'Abnormal patterns detected'.
          APPEND wa_return_status TO t_return_status.

          lv_call_id_char = l_result-call_id.


          " Open long anomalies count

        WHEN  'LONG_ANML' .

          lv_events_count = 0.

          SELECT COUNT(*) INTO lv_events_count FROM zsmartmonitorlog
                    WHERE context_name IN lr_sys_context_names
                    AND source = 'A'
                    AND suppressed <> 'X'
                    AND duration > 3600.

          IF ( sy-subrc <> 0 ).
            lv_events_count = 0.
          ENDIF.

          lv_events_count_char = lv_events_count.

          l_parameter-param = 'SYS_ID'.
          l_parameter-value = <ls_sel_opt_sid>-sid.
          APPEND l_parameter TO t_parameter.
          l_result-result-t_parameter = t_parameter.

          l_result-result-text = lv_events_count_char.
          CONDENSE l_result-result-text.

          l_result-result-count = 1.
          l_result-result-average =
          l_result-result-min =
          l_result-result-max =
          l_result-result-sum = lv_events_count_char.

          l_result-call_id = <ls_selopt_para>-call_id.

          APPEND l_result TO t_result.

          wa_return_status-call_id = <ls_selopt_para>-call_id.
          wa_return_status-status = 0.
          wa_return_status-msgtext = 'Long anomalies detected'.
          APPEND wa_return_status TO t_return_status.

          lv_call_id_char = l_result-call_id.

          " Count amount of all anomalies in the moment

        WHEN  'ALL_AN_CNT' .

          lv_events_count = 0.

          SELECT COUNT(*) INTO lv_events_count FROM zsmartmonitorlog
                     WHERE event_date = sy-datum
                     AND  context_name IN lr_sys_context_names
                     AND source = 'A'.


          IF ( sy-subrc <> 0 ).
            lv_events_count = 0.
          ENDIF.

          lv_events_count_char = lv_events_count.

          l_parameter-param = 'SYS_ID'.
          l_parameter-value = <ls_sel_opt_sid>-sid.
          APPEND l_parameter TO t_parameter.
          l_result-result-t_parameter = t_parameter.

          l_result-result-text = lv_events_count_char.
          CONDENSE l_result-result-text.

          l_result-result-count = 1.
          l_result-result-average =
          l_result-result-min =
          l_result-result-max =
          l_result-result-sum = lv_events_count_char.

          l_result-call_id = <ls_selopt_para>-call_id.

          APPEND l_result TO t_result.

          wa_return_status-call_id = <ls_selopt_para>-call_id.
          wa_return_status-status = 0.
          wa_return_status-msgtext = 'Abnormal patterns detected'.
          APPEND wa_return_status TO t_return_status.

          lv_call_id_char = l_result-call_id.


        WHEN  'CLOSURE_SPEED' .

          lv_events_count = 0.
          lv_sum_duration = 0.
          " Getting amount of closed anomalies for today
          SELECT COUNT(*) INTO lv_events_count FROM zsmartmonitorlog
                     WHERE event_date = sy-datum
                     AND context_name IN lr_sys_context_names
                     AND source = 'A'
                     AND suppressed = 'X'.


          IF ( sy-subrc <> 0 ).
            lv_events_count = 0.
          ENDIF.

          " Getting sum of duration for closed alerts

          SELECT SUM( duration ) INTO lv_sum_duration
             FROM zsmartmonitorlog WHERE event_date = sy-datum
             AND context_name IN lr_sys_context_names
             AND source = 'A'
             AND suppressed = 'X'.

          " Calculating average closure speed in minutes

          IF ( lv_events_count <> 0 ).
            lv_avg_duration = ( lv_sum_duration / lv_events_count ) / 60.
          ELSE.
            lv_avg_duration = 0.
          ENDIF.

          lv_avg_duration_char = lv_avg_duration.

          l_parameter-param = 'SYS_ID'.
          l_parameter-value = <ls_sel_opt_sid>-sid.
          APPEND l_parameter TO t_parameter.
          l_result-result-t_parameter = t_parameter.

          l_result-result-text = lv_avg_duration_char.
          CONDENSE l_result-result-text.

          l_result-result-count = 1.
          l_result-result-average =
          l_result-result-min =
          l_result-result-max =
          l_result-result-sum = lv_avg_duration_char.

          l_result-call_id = <ls_selopt_para>-call_id.

          APPEND l_result TO t_result.

          wa_return_status-call_id = <ls_selopt_para>-call_id.
          wa_return_status-status = 0.
          wa_return_status-msgtext = 'Abnormal patterns detected'.
          APPEND wa_return_status TO t_return_status.

          lv_call_id_char = l_result-call_id.


      ENDCASE. " CASE lv_key_fig

    ELSE.

      l_parameter-param = 'SYS_ID'.
      l_parameter-value = <ls_sel_opt_sid>-sid.
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

      wa_return_status-call_id = <ls_selopt_para>-call_id.
      wa_return_status-status = 0.
      wa_return_status-msgtext = 'No anomalies data found'.
      APPEND wa_return_status TO t_return_status.

      lv_call_id_char = l_result-call_id.



    ENDIF. " IF lr_sys_context_names IS NOT INITIAL

  ENDLOOP. " LOOP AT lt_sel_opt_sid ASSIGNING FIELD-SYMBOL(<ls_sel_opt_sid>)

  APPEND LINES OF t_result        TO result.
  APPEND LINES OF t_return_status TO return_status.

ENDFUNCTION.