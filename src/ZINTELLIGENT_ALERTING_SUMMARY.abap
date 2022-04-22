*&---------------------------------------------------------------------*
*& Report  ZINTELLIGENT_ALERTING_SUMMARY
*&
*&---------------------------------------------------------------------*
*&
*&
*&---------------------------------------------------------------------*
REPORT zintelligent_alerting_summary.

DATA lv_records_counter LIKE sy-tabix.
DATA lv_today TYPE sy-datum.
DATA lv_processing_time TYPE ace_d_event_value_sum.
DATA lv_exec_time_start TYPE i.
DATA lv_exec_time_end TYPE i.

" -------------------------- Structures for database output --------------------------

" Active anomalies

TYPES: BEGIN OF ty_active_anomalies_tt,
         context_name   TYPE ac_context_name,
         metric_details TYPE string,
         event_date     TYPE dats,
         event_time     TYPE char8,
       END OF ty_active_anomalies_tt.
DATA lt_active_anomalies TYPE STANDARD TABLE OF ty_active_anomalies_tt.


" Long anomalies

TYPES: BEGIN OF ty_long_anomalies_tt,
         context_name   TYPE ac_context_name,
         metric_details TYPE string,
         event_date     TYPE dats,
         event_time     TYPE char8,
         duration       TYPE int4,
         duration_hours TYPE ace_d_event_value_sum,
       END OF ty_long_anomalies_tt.

DATA lt_long_anomalies TYPE STANDARD TABLE OF ty_long_anomalies_tt.
DATA wa_long_anomalies TYPE ty_long_anomalies_tt.


" Longest anomalies

TYPES: BEGIN OF ty_longest_anomalies_tt,
         context_name   TYPE ac_context_name,
         metric_details TYPE string,
         event_date     TYPE dats,
         event_time     TYPE char8,
         duration       TYPE int4,
         suppressed     TYPE char1,
         duration_hours TYPE ace_d_event_value_sum,
       END OF ty_longest_anomalies_tt.

DATA lt_longest_anomalies TYPE STANDARD TABLE OF ty_longest_anomalies_tt.
DATA wa_longest_anomalies TYPE ty_longest_anomalies_tt.


" Frequent anomalies

TYPES: BEGIN OF ty_frequent_anomalies_tt,
         context_name   TYPE ac_context_name,
         metric_tname   TYPE ac_name,
         count          TYPE int4,
         metric_details TYPE string,

       END OF ty_frequent_anomalies_tt.

DATA lt_frequent_anomalies TYPE STANDARD TABLE OF ty_frequent_anomalies_tt.
DATA wa_frequent_anomalies TYPE ty_frequent_anomalies_tt.

" -------------------------- Structures for ALV Grid --------------------------

DATA fieldcatalog TYPE slis_t_fieldcat_alv WITH HEADER LINE.

DATA: lv_repid   TYPE sy-repid,
      wa_layout  TYPE slis_layout_alv,
      lt_events1 TYPE slis_t_event,
      lt_events2 TYPE slis_t_event,
      lt_events3 TYPE slis_t_event,
      lt_events4 TYPE slis_t_event,
      wa_events  TYPE slis_alv_event.

" -------------------------- Data selection start --------------------------

lv_today = sy-datum.

PERFORM selectsummarydata.

" Active anomalies

*SELECT context_name metric_details event_date event_time
*  INTO TABLE lt_active_anomalies FROM zsmartmonitorlog WHERE event_date = lv_today AND source = 'A' AND suppressed <> 'X'.
*
*" Anomalies longer than one hour
*
*SELECT context_name metric_details event_date event_time duration suppressed
*  INTO TABLE lt_long_anomalies FROM zsmartmonitorlog UP TO 10 rows
*  WHERE source = 'A' AND duration > 3600 ORDER BY duration descending.

" -------------------------- Data selection end --------------------------

" -------------------------- ALV Grid design start --------------------------

lv_repid = sy-repid.
wa_layout-colwidth_optimize = 'X'.

wa_events-name = 'TOP_OF_PAGE'.
wa_events-form = 'ACTIVE_ANOMALIES'.
APPEND wa_events TO lt_events1.
CLEAR wa_events.

wa_events-name = 'TOP_OF_PAGE'.
wa_events-form = 'LONG_ANOMALIES'.
APPEND wa_events TO lt_events2.
CLEAR wa_events.


wa_events-name = 'TOP_OF_PAGE'.
wa_events-form = 'LONGEST_ANOMALIES'.
APPEND wa_events TO lt_events3.
CLEAR wa_events.

wa_events-name = 'TOP_OF_PAGE'.
wa_events-form = 'FREQUENT_ANOMALIES'.
APPEND wa_events TO lt_events4.
CLEAR wa_events.

CALL FUNCTION 'REUSE_ALV_BLOCK_LIST_INIT'
  EXPORTING
    i_callback_program       = lv_repid
    i_callback_pf_status_set = 'SUB_PF_STATUS'
    i_callback_user_command  = 'USER_COMMAND'.

" Active anomalies

fieldcatalog-fieldname   = 'context_name'.
fieldcatalog-seltext_m   = 'Object'.
fieldcatalog-col_pos     = 1.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'metric_details'.
fieldcatalog-seltext_m   = 'Metric'.
fieldcatalog-col_pos     = 2.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'event_date'.
fieldcatalog-seltext_m   = 'Date'.
fieldcatalog-col_pos     = 3.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'event_time'.
fieldcatalog-seltext_m   = 'Time'.
fieldcatalog-col_pos     = 4.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

CALL FUNCTION 'REUSE_ALV_BLOCK_LIST_APPEND'
  EXPORTING
    is_layout   = wa_layout
    it_fieldcat = fieldcatalog[]
    i_tabname   = ''
    it_events   = lt_events1
  TABLES
    t_outtab    = lt_active_anomalies.

REFRESH fieldcatalog[].

" Long anomalies

fieldcatalog-fieldname   = 'context_name'.
fieldcatalog-seltext_m   = 'Object'.
fieldcatalog-col_pos     = 1.
fieldcatalog-outputlen     = '40'.

APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'metric_details'.
fieldcatalog-seltext_m   = 'Metric'.
fieldcatalog-col_pos     = 2.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'event_date'.
fieldcatalog-seltext_m   = 'Date'.
fieldcatalog-col_pos     = 3.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'event_time'.
fieldcatalog-seltext_m   = 'Time'.
fieldcatalog-col_pos     = 4.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'duration_hours'.
fieldcatalog-seltext_m   = 'Duration (hours)'.
fieldcatalog-col_pos     = 5.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.


CALL FUNCTION 'REUSE_ALV_BLOCK_LIST_APPEND'
  EXPORTING
    is_layout   = wa_layout
    it_fieldcat = fieldcatalog[]
    i_tabname   = ''
    it_events   = lt_events2
  TABLES
    t_outtab    = lt_long_anomalies.

REFRESH fieldcatalog[].


" Longest anomalies

fieldcatalog-fieldname   = 'context_name'.
fieldcatalog-seltext_m   = 'Object'.
fieldcatalog-col_pos     = 1.
fieldcatalog-outputlen     = '40'.

APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'metric_details'.
fieldcatalog-seltext_m   = 'Metric'.
fieldcatalog-col_pos     = 2.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'event_date'.
fieldcatalog-seltext_m   = 'Date'.
fieldcatalog-col_pos     = 3.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'event_time'.
fieldcatalog-seltext_m   = 'Time'.
fieldcatalog-col_pos     = 4.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'duration_hours'.
fieldcatalog-seltext_m   = 'Duration'.
fieldcatalog-col_pos     = 5.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'suppressed'.
fieldcatalog-seltext_m   = 'Closed'.
fieldcatalog-col_pos     = 6.
fieldcatalog-outputlen     = '10'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.



CALL FUNCTION 'REUSE_ALV_BLOCK_LIST_APPEND'
  EXPORTING
    is_layout   = wa_layout
    it_fieldcat = fieldcatalog[]
    i_tabname   = ''
    it_events   = lt_events3
  TABLES
    t_outtab    = lt_longest_anomalies.

REFRESH fieldcatalog[].


" Frequent anomalies

fieldcatalog-fieldname   = 'context_name'.
fieldcatalog-seltext_m   = 'Object'.
fieldcatalog-col_pos     = 1.
fieldcatalog-outputlen     = '40'.

APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'metric_details'.
fieldcatalog-seltext_m   = 'Metric'.
fieldcatalog-col_pos     = 2.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.

fieldcatalog-fieldname   = 'count'.
fieldcatalog-seltext_m   = 'Count'.
fieldcatalog-col_pos     = 3.
fieldcatalog-outputlen     = '40'.
APPEND fieldcatalog TO fieldcatalog.
CLEAR  fieldcatalog.


CALL FUNCTION 'REUSE_ALV_BLOCK_LIST_APPEND'
  EXPORTING
    is_layout   = wa_layout
    it_fieldcat = fieldcatalog[]
    i_tabname   = ''
    it_events   = lt_events4
  TABLES
    t_outtab    = lt_frequent_anomalies.

GET RUN TIME FIELD lv_exec_time_end.

CALL FUNCTION 'REUSE_ALV_BLOCK_LIST_DISPLAY'.




FORM active_anomalies.

  DATA lv_processing_time_char TYPE char10.

  lv_processing_time_char = lv_processing_time.

  CONDENSE lv_processing_time_char.

  WRITE:/ 'Intelligent Alerting Summary' COLOR 3, sy-datum  COLOR 3, sy-uzeit COLOR 3.
  WRITE:/  'Selection time: ', lv_processing_time_char.
  WRITE /.
  WRITE:/ 'Active anomalies' COLOR 5.
ENDFORM.

FORM long_anomalies.
  WRITE:/ 'Active anomalies longer than one hour' COLOR 5.
ENDFORM.

FORM longest_anomalies.
  WRITE:/ 'Top 10 longest anomalies' COLOR 5.
ENDFORM.

FORM frequent_anomalies.
  WRITE:/ 'Top 10 frequent anomalies' COLOR 5.
ENDFORM.

FORM sub_pf_status USING rt_extab TYPE slis_t_extab.
  SET PF-STATUS 'ZSTANDARD'.
ENDFORM.


FORM selectsummarydata.

  GET RUN TIME FIELD lv_exec_time_start.

  " Active anomalies

  SELECT context_name metric_details event_date event_time
    INTO TABLE lt_active_anomalies FROM zsmartmonitorlog WHERE event_date = lv_today AND source = 'A' AND suppressed <> 'X'.

  " Anomalies longer than one hour

  SELECT context_name metric_details event_date event_time duration
    INTO TABLE lt_long_anomalies FROM zsmartmonitorlog "UP TO 10 ROWS
    WHERE source = 'A' AND  suppressed <> 'X' AND duration > 3600 ORDER BY duration DESCENDING.



  " Filling duration in hours

  LOOP AT lt_long_anomalies INTO wa_long_anomalies.

    wa_long_anomalies-duration_hours = wa_long_anomalies-duration / 3600.

    MODIFY lt_long_anomalies FROM wa_long_anomalies.

  ENDLOOP.

  " Top 10 longest anomalies

  SELECT context_name metric_details event_date event_time duration suppressed
    INTO TABLE lt_longest_anomalies FROM zsmartmonitorlog UP TO 10 ROWS
     WHERE source = 'A' ORDER BY duration DESCENDING.

  " Filling duration in hours

  LOOP AT lt_longest_anomalies INTO wa_longest_anomalies.

    wa_longest_anomalies-duration_hours = wa_longest_anomalies-duration / 3600.

    MODIFY lt_longest_anomalies FROM wa_longest_anomalies.

  ENDLOOP.

  " Top 10 frequent

  SELECT context_name metric_tname  COUNT(*)  FROM zsmartmonitorlog
    INTO  TABLE lt_frequent_anomalies
     WHERE source = 'A' GROUP BY context_name metric_tname.

  SORT lt_frequent_anomalies BY count DESCENDING.


  " Leaving only 10 records in table

  DESCRIBE TABLE lt_frequent_anomalies LINES lv_records_counter.

  IF lv_records_counter > 10.
    DELETE lt_frequent_anomalies FROM 11 TO lv_records_counter.
  ENDIF.

  " Adding metric full name

  LOOP AT lt_frequent_anomalies INTO wa_frequent_anomalies.

    SELECT SINGLE metric_details FROM zsmartmonitorlog INTO wa_frequent_anomalies-metric_details
      WHERE context_name = wa_frequent_anomalies-context_name AND metric_tname = wa_frequent_anomalies-metric_tname.

    MODIFY lt_frequent_anomalies FROM wa_frequent_anomalies.

  ENDLOOP. " LOOP AT lt_frequent_anomalies INTO wa_frequent_anomalies

    GET RUN TIME FIELD lv_exec_time_end.

  lv_processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

ENDFORM.



FORM user_command USING r_ucomm    LIKE sy-ucomm
                       st_selfield TYPE slis_selfield.

  IF ( r_ucomm EQ '&NTE' ) OR ( r_ucomm EQ '%REFRESH' ) .

    PERFORM selectsummarydata.
    st_selfield-refresh = 'X'.

  ENDIF.

ENDFORM.  "User_command