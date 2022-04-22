*&---------------------------------------------------------------------*
*& Report  ZINTELLIGENT_ALERTS_ANALYSIS
*&
*&---------------------------------------------------------------------*
*&
*&
*&---------------------------------------------------------------------*
report zintelligent_alerts_analysis.

" ------------------------------ Data types for reporting ----------------

types: begin of ty_analysis_summary_tt,
         event_date            type dats,
         suppressed_y_no_notif type int4,
         suppressed_y_notif    type int4,
         suppressed_r_no_notif type int4,
         suppressed_r_notif    type int4,
         approved_y_no_notif   type int4,
         approved_y_notif      type int4,
         approved_r_no_notif   type int4,
         approved_r_notif      type int4,
         suppressed_count      type int4,
         approved_count        type int4,
         suppressed_pcnt       type p decimals 2,
         approved_pcnt         type p decimals 2,
       end of ty_analysis_summary_tt.

data lt_analysis_summary type standard table of ty_analysis_summary_tt.
data wa_analysis_summary type ty_analysis_summary_tt.

types: begin of ty_analysis_summary_sup_tt,
         event_date       type dats,
         rating           type char10,
         notif_active     type char1,
         suppressed_count type int4,
       end of ty_analysis_summary_sup_tt.

data lt_analysis_summary_sup type standard table of ty_analysis_summary_sup_tt.
field-symbols <ls_analysis_summary_sup> like line of lt_analysis_summary_sup.

types: begin of ty_analysis_summary_app_tt,
         event_date     type dats,
         rating         type char10,
         notif_active   type char1,
         approved_count type int4,
       end of ty_analysis_summary_app_tt.

data lt_analysis_summary_app type standard table of ty_analysis_summary_app_tt.
field-symbols <ls_analysis_summary_app> like line of lt_analysis_summary_app.

types: begin of ty_alerts_by_context_tt,
         context_name type ac_context_name,
         alert_name   type string,
         count        type int4,
       end of ty_alerts_by_context_tt.

data lt_alerts_by_context type standard table of ty_alerts_by_context_tt with key  context_name alert_name.
data wa_alerts_by_context type ty_alerts_by_context_tt.

data lt_alerts_by_context_group_s type standard table of ty_alerts_by_context_tt.
data lt_alerts_by_context_group_a type standard table of ty_alerts_by_context_tt.


" ------------------- Data types for logic and ALV grid output ----------------

data lv_records_counter like sy-tabix.

data: lv_date_iterator type dats,
      lv_date_end      type dats.


data fieldcatalog type slis_t_fieldcat_alv with header line.
data wa_layout  type slis_layout_alv.
data lt_events type slis_t_event.
data wa_events  type slis_alv_event.

data lv_current_date type char8.

types: begin of ty_context_name,
         context_name type zsmartmonitorlog-context_name,
       end of ty_context_name.

data : lt_context_name type table of ty_context_name,
       wa_context_name type ty_context_name.

data lv_exec_time_start type i.
data lv_exec_time_end type i.
data lv_processing_time type ace_d_event_value_sum.

data lv_approved_count type int4.
data lv_suppressed_count type int4.

data lv_approved_count_total type int4.
data lv_suppressed_count_total type int4.

" ------------------- Input parameters ----------------

selection-screen begin of block data_selector with frame title text-000.

parameters datefrom type dats obligatory.                                 " Date from
parameters dateto   type dats obligatory.                                 " Date to
parameters cntxname  type ty_context_name-context_name modif id mo.       " Context name

selection-screen end of block data_selector.

selection-screen begin of block execution_options with frame title text-001.

parameters total    radiobutton group rb1 default 'X' user-command radio.  " Alert analysis summary
parameters supcntxa radiobutton group rb1.                                 " Suppression by context aggregated
parameters supcntxd radiobutton group rb1.                                 " Suppression by context details
parameters appcntxa radiobutton group rb1.                                 " Approvals by context aggregated
parameters appcntxd radiobutton group rb1.                                 " Approvals by context
parameters toplists radiobutton group rb1.                                 " Approvals by context
parameters config   radiobutton group rb1.                                 " Display configuration


selection-screen end of block execution_options.



initialization.

  datefrom = '20210101'.
  lv_current_date = sy-datum.
  dateto = lv_current_date.

  " Disabling context name input when no detalization mode selected

at selection-screen output.
  loop at screen.

    if ( total = 'X' )
        or ( supcntxa = 'X' )
            or ( appcntxa = 'X' )
              or ( toplists = 'X' )
                 or ( config = 'X' ).

      if screen-group1 = 'MO'.
        screen-invisible = 1.
        screen-input     = 0.
      endif.
    endif.
    modify screen.
  endloop.

  " Filling context name F4 help value

at selection-screen on value-request for cntxname.

  select context_name from zsmartmonitorlog into table lt_context_name where context_name <> ''.

  delete adjacent duplicates from lt_context_name.

  call function 'F4IF_INT_TABLE_VALUE_REQUEST'
    exporting
      retfield    = 'CONTEXT_NAME'
      dynpprog    = sy-repid
      dynpnr      = sy-dynnr
      dynprofield = 'CNTXNAME'
      value_org   = 'S'
    tables
      value_tab   = lt_context_name.

start-of-selection.

  wa_layout-colwidth_optimize = 'X'.

  " Selecting routine depending on selection

  if total eq abap_true.
    perform alert_analysis_summary.
  endif.

  if supcntxa eq abap_true.
    perform by_context_aggregated using 'false'.

  endif.

  if supcntxd eq abap_true.
    perform by_context_detailed using 'false' cntxname.
  endif.

  if appcntxa eq abap_true.
    perform by_context_aggregated using 'true'.

  endif.

  if appcntxd eq abap_true.

    perform by_context_detailed using 'true' cntxname.

  endif.

  if toplists eq abap_true.
    perform top_lists.
  endif.

  if config eq abap_true.
    perform display_config.
  endif.



  "------------------------------------------- Display configuration ----------------------------------------------

form display_config.

  data: it_listheader type slis_t_listheader.

  types: begin of ty_lt_ctx_alert_id_tt,
           context_id    like acreactiondir-context_id,
           alert_type_id like acreactiondir-mea_type_id,

         end of ty_lt_ctx_alert_id_tt.

  data lt_ctx_alert_id type standard table of ty_lt_ctx_alert_id_tt.
  field-symbols <ls_ctx_alert_id> like line of lt_ctx_alert_id.


  types: begin of ty_lt_ctx_alert_name_tt,
           context_name type ac_context_name,
           alert_name   type string,
         end of ty_lt_ctx_alert_name_tt.

  data lt_ctx_alert_name type standard table of ty_lt_ctx_alert_name_tt.
  data wa_ctx_alert_name type  ty_lt_ctx_alert_name_tt.

  get run time field lv_exec_time_start.

  select context_id mea_type_id into table lt_ctx_alert_id from acreactiondir
    where reaction_id = 'ZSMARTALERTS' and workmode_id = ''.

  loop at lt_ctx_alert_id assigning <ls_ctx_alert_id>.

    select single context_name into wa_ctx_alert_name-context_name from v_acentrypoints
       where context_id = <ls_ctx_alert_id>-context_id.

    select single short_text into wa_ctx_alert_name-alert_name from acalertdirt
       where context_id = <ls_ctx_alert_id>-context_id and alert_type_id = <ls_ctx_alert_id>-alert_type_id and langu = 'E'.

    append wa_ctx_alert_name to lt_ctx_alert_name.

  endloop. "LOOP AT lt_ctx_alert ASSIGNING <ls_ctx_alert>

  fieldcatalog-fieldname   = 'CONTEXT_NAME'.
  fieldcatalog-seltext_m   = 'Context name'.
  fieldcatalog-col_pos     = 1.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'ALERT_NAME'.
  fieldcatalog-seltext_m   = 'Alert name'.
  fieldcatalog-col_pos     = 2.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  wa_layout-colwidth_optimize = 'X'.

  wa_events-name = 'TOP_OF_PAGE'.
  wa_events-form = 'TOP_CONFIG'.
  append wa_events to lt_events.
  clear wa_events.


  call function 'REUSE_ALV_GRID_DISPLAY'
    exporting
      i_callback_program     = sy-repid
      it_fieldcat            = fieldcatalog[]
      is_layout              = wa_layout
      i_callback_top_of_page = 'TOP_CONFIG'
    tables
      t_outtab               = lt_ctx_alert_name.

  refresh fieldcatalog[].

endform. " display_config



"------------------------------------------- Alert Analysis Summary ----------------------------------------------

form alert_analysis_summary.

  get run time field lv_exec_time_start.

  select event_date rating notif_active count(*) from zsmartmonitorlog into table lt_analysis_summary_sup
    where source = 'T'
    and suppressed = 'X'
    and event_date ge datefrom
    and event_date le dateto
    group by event_date rating notif_active.

  select event_date rating notif_active count(*) from zsmartmonitorlog into table lt_analysis_summary_app
    where source = 'T'
    and anomaly_detected = 'X'
    and event_date ge datefrom
    and event_date le dateto
    group by event_date rating notif_active.

  " Preparing dates table

  lv_date_iterator = datefrom.
  lv_date_end = dateto.

  while lv_date_iterator le lv_date_end.

    clear wa_analysis_summary.

    unassign <ls_analysis_summary_sup>.
    unassign <ls_analysis_summary_app>.

    lv_approved_count = 0.
    lv_suppressed_count = 0.

    wa_analysis_summary-event_date = lv_date_iterator.


    "read table lt_analysis_summary_sup with key event_date = lv_date_iterator assigning <ls_analysis_summary_sup>.

    loop at  lt_analysis_summary_sup  assigning <ls_analysis_summary_sup> where event_date = lv_date_iterator .

      if ( <ls_analysis_summary_sup> is assigned ).

        lv_suppressed_count = lv_suppressed_count + <ls_analysis_summary_sup>-suppressed_count.

        case <ls_analysis_summary_sup>-rating.

          when 'Red'.

            if ( <ls_analysis_summary_sup>-notif_active is not initial ).
              wa_analysis_summary-suppressed_r_notif = <ls_analysis_summary_sup>-suppressed_count.
            else.
              wa_analysis_summary-suppressed_r_no_notif = <ls_analysis_summary_sup>-suppressed_count.
            endif.

          when 'Yellow'.

            if ( <ls_analysis_summary_sup>-notif_active is not initial ).
              wa_analysis_summary-suppressed_y_notif = <ls_analysis_summary_sup>-suppressed_count.
            else.
              wa_analysis_summary-suppressed_y_no_notif = <ls_analysis_summary_sup>-suppressed_count.
            endif.

          when others.

            if ( <ls_analysis_summary_sup>-notif_active is not initial ).
              wa_analysis_summary-suppressed_y_notif = <ls_analysis_summary_sup>-suppressed_count.
            else.
              wa_analysis_summary-suppressed_y_no_notif = <ls_analysis_summary_sup>-suppressed_count.
            endif.

        endcase.

      endif. " IF ( <ls_analysis_summary_sup> IS ASSIGNED )

    endloop. "LOOP AT  lt_analysis_summary_sup  assigning <ls_analysis_summary_sup> WHERE event_date = lv_date_iterator

    loop at  lt_analysis_summary_app  assigning <ls_analysis_summary_app> where event_date = lv_date_iterator .

      if ( <ls_analysis_summary_app> is assigned ).

        lv_approved_count = lv_approved_count + <ls_analysis_summary_app>-approved_count.

        case <ls_analysis_summary_app>-rating.

          when 'Red'.

            if ( <ls_analysis_summary_app>-notif_active is not initial ).
              wa_analysis_summary-approved_r_notif = <ls_analysis_summary_app>-approved_count.
            else.
              wa_analysis_summary-approved_r_no_notif = <ls_analysis_summary_app>-approved_count.
            endif.

          when 'Yellow'.

            if ( <ls_analysis_summary_app>-notif_active is not initial ).
              wa_analysis_summary-approved_y_notif = <ls_analysis_summary_app>-approved_count.
            else.
              wa_analysis_summary-approved_y_no_notif = <ls_analysis_summary_app>-approved_count.
            endif.

          when others.

            if ( <ls_analysis_summary_app>-notif_active is not initial ).
              wa_analysis_summary-approved_y_notif = <ls_analysis_summary_app>-approved_count.
            else.
              wa_analysis_summary-approved_y_no_notif = <ls_analysis_summary_app>-approved_count.
            endif.

        endcase.

      endif. " IF ( <ls_analysis_summary_sup> IS ASSIGNED )

    endloop. "LOOP AT  lt_analysis_summary_sup  assigning <ls_analysis_summary_sup> WHERE event_date = lv_date_iterator


    wa_analysis_summary-approved_count = lv_approved_count.
    wa_analysis_summary-suppressed_count = lv_suppressed_count.

    lv_approved_count_total  = lv_approved_count_total  + lv_approved_count.
    lv_suppressed_count_total  = lv_suppressed_count_total  + lv_suppressed_count.

    wa_analysis_summary-approved_pcnt = ( lv_approved_count / ( lv_approved_count + lv_suppressed_count ) ) * 100.
    wa_analysis_summary-suppressed_pcnt = ( lv_suppressed_count / ( lv_approved_count + lv_suppressed_count ) ) * 100.

    append wa_analysis_summary to lt_analysis_summary.

    lv_date_iterator = lv_date_iterator + 1.

  endwhile. " while lv_date_iterator le lv_date_end

  if lt_analysis_summary is not initial.

    fieldcatalog-fieldname   = 'EVENT_DATE'.
    fieldcatalog-seltext_m   = 'Date'.
    fieldcatalog-col_pos     = 1.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'SUPPRESSED_COUNT'.
    fieldcatalog-seltext_m   = 'Total False'.
    fieldcatalog-emphasize = 'C600'.
    fieldcatalog-col_pos     = 2.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'APPROVED_COUNT'.
    fieldcatalog-seltext_m   = 'Total True'.
    fieldcatalog-emphasize = 'C500'.
    fieldcatalog-col_pos     = 3.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'SUPPRESSED_PCNT'.
    fieldcatalog-seltext_m   = 'Total False (%)'.
    fieldcatalog-emphasize = 'C600'.
    fieldcatalog-col_pos     = 4.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'APPROVED_PCNT'.
    fieldcatalog-seltext_m   = 'Total True (%)'.
    fieldcatalog-col_pos     = 5.
    fieldcatalog-emphasize = 'C500'.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'SUPPRESSED_R_NOTIF'.
    fieldcatalog-seltext_m   = 'False Red w/notif'.
    fieldcatalog-col_pos     = 6.
    fieldcatalog-emphasize = 'C600'.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'SUPPRESSED_R_NO_NOTIF'.
    fieldcatalog-seltext_m   = 'False Red wout/notif'.
    fieldcatalog-emphasize = 'C600'.
    fieldcatalog-col_pos     = 7.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'SUPPRESSED_Y_NOTIF'.
    fieldcatalog-seltext_m   = 'False Yellow w/notif'.
    fieldcatalog-emphasize = 'C600'.
    fieldcatalog-col_pos     = 8.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'SUPPRESSED_Y_NO_NOTIF'.
    fieldcatalog-seltext_m   = 'False Yellow wout/notif'.
    fieldcatalog-emphasize = 'C600'.
    fieldcatalog-col_pos     = 9.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'APPROVED_R_NOTIF'.
    fieldcatalog-seltext_m   = 'True Red w/notif'.
    fieldcatalog-emphasize = 'C500'.
    fieldcatalog-col_pos     = 10.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'APPROVED_R_NO_NOTIF'.
    fieldcatalog-seltext_m   = 'True Red wout/notif'.
    fieldcatalog-emphasize = 'C500'.
    fieldcatalog-col_pos     = 11.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'APPROVED_Y_NOTIF'.
    fieldcatalog-seltext_m   = 'True Yellow w/notif'.
    fieldcatalog-emphasize = 'C500'.
    fieldcatalog-col_pos     = 12.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    fieldcatalog-fieldname   = 'APPROVED_Y_NO_NOTIF'.
    fieldcatalog-seltext_m   = 'True Yellow wout/notif'.
    fieldcatalog-emphasize = 'C500'.
    fieldcatalog-col_pos     = 13.
    append fieldcatalog to fieldcatalog.
    clear  fieldcatalog.

    wa_layout-colwidth_optimize = 'X'.

    call function 'REUSE_ALV_GRID_DISPLAY'
      exporting
        i_callback_program     = sy-repid
        it_fieldcat            = fieldcatalog[]
        is_layout              = wa_layout
        "it_events   = lt_events
        i_callback_top_of_page = 'TOP_OF_PAGE'
      tables
        t_outtab               = lt_analysis_summary.

    refresh fieldcatalog[].

  endif. "IF lt_analysis_summary IS NOT INITIAL.

endform. " alert_analysis_summary

"--------------------------------------- By context (aggregated) -----------------------------------------

form by_context_aggregated using ip_mode.

  types: begin of ty_lt_ctx_aggr_tt,
           event_date   type dats,
           context_name type ac_context_name,
           count        type int4,
         end of ty_lt_ctx_aggr_tt.

  data lt_ctx_aggr type standard table of ty_lt_ctx_aggr_tt.
  field-symbols <ls_ctx_aggr> like line of lt_ctx_aggr.

  types: begin of ty_context_tt,
           context_name type ac_context_name,
           count        type int4,
         end of ty_context_tt.

  data lt_context type standard table of ty_context_tt.
  data wa_context type ty_context_tt.


  field-symbols: <dyn_table> type standard table,
                 <dyn_wa>.

  data: alv_fldcat type slis_t_fieldcat_alv,
        it_fldcat  type lvc_t_fcat.

  data lv_dats_iterator type char10.

  data: wa_cat like line of alv_fldcat.


  data: gv_fldname(20) type c.

  data: new_table    type ref to data,
        new_line     type ref to data,
        wa_it_fldcat type lvc_s_fcat.

  data: fieldname(20) type c.
  data: fieldvalue(128) type c.
  field-symbols: <fs1>.

  data lv_count type int4.

  data wa_layout  type slis_layout_alv.

  get run time field lv_exec_time_start.

  if ip_mode = 'false'.

    select event_date context_name count(*) from zsmartmonitorlog into table lt_ctx_aggr
    where source = 'T'
    and suppressed = 'X'
    and event_date ge datefrom
    and event_date le dateto
    group by event_date context_name.

  else.
    select event_date context_name count(*) from zsmartmonitorlog into table lt_ctx_aggr
   where source = 'T'
   and anomaly_detected = 'X'
   and event_date ge datefrom
   and event_date le dateto
   group by event_date context_name.
  endif.

  " Preparing table of contexts and totals

  loop at lt_ctx_aggr assigning <ls_ctx_aggr>.

    clear wa_context.

    read table lt_context with key context_name = <ls_ctx_aggr>-context_name into wa_context.

    if ( sy-subrc <> 0 and wa_context is initial ).

      wa_context-context_name = <ls_ctx_aggr>-context_name.
      wa_context-count = <ls_ctx_aggr>-count.
      append wa_context to lt_context.

    else.
      wa_context-count = wa_context-count + <ls_ctx_aggr>-count.
      modify lt_context from wa_context transporting count where context_name = <ls_ctx_aggr>-context_name.

    endif.

  endloop.

  lv_date_iterator = datefrom.
  lv_date_end = dateto.

  wa_it_fldcat-fieldname = 'CONTEXT_NAME'.
  wa_it_fldcat-datatype = 'CHAR'.
  wa_it_fldcat-inttype = 'C'.
  wa_it_fldcat-intlen = '000128'.

  wa_cat-fieldname = 'CONTEXT_NAME'.
  wa_cat-seltext_s = 'Object'.
  wa_cat-outputlen = '40'.
  wa_cat-fix_column = 'X'.

  append wa_cat to alv_fldcat.

  append wa_it_fldcat to it_fldcat .
  clear wa_it_fldcat.

  clear wa_cat.

  wa_it_fldcat-fieldname = 'COUNT'.
  wa_it_fldcat-datatype = 'INT4'.

  wa_cat-fieldname = 'COUNT'.
  wa_cat-seltext_s = 'Total'.
  wa_cat-outputlen = '8'.

  if ip_mode = 'false'.
    wa_cat-emphasize = 'C600'.
  else.
    wa_cat-emphasize = 'C500'.
  endif.

  append wa_cat to alv_fldcat.

  append wa_it_fldcat to it_fldcat .
  clear wa_it_fldcat.

  clear wa_cat.

  while lv_date_iterator le lv_date_end.

    concatenate lv_date_iterator+6(2) '-' lv_date_iterator+4(2) '-' lv_date_iterator(4) into lv_dats_iterator.

    clear wa_it_fldcat.

    wa_it_fldcat-fieldname = lv_dats_iterator.
    wa_it_fldcat-datatype = 'INT4'.
    append wa_it_fldcat to it_fldcat .

    wa_cat-fieldname = wa_it_fldcat-fieldname.
    wa_cat-seltext_s = wa_it_fldcat-fieldname.
    wa_cat-outputlen = '8'.
    append wa_cat to alv_fldcat.

    lv_date_iterator = lv_date_iterator + 1.

  endwhile.

  call method cl_alv_table_create=>create_dynamic_table
    exporting
      it_fieldcatalog = it_fldcat
    importing
      ep_table        = new_table.

  assign new_table->* to <dyn_table>.

  create data new_line like line of <dyn_table>.
  assign new_line->* to <dyn_wa>.


  clear lv_dats_iterator.

  loop at lt_context assigning field-symbol(<ls_context>).

    fieldname = 'CONTEXT_NAME'.
    fieldvalue = <ls_context>-context_name.
    assign component fieldname of structure <dyn_wa> to <fs1>.
    <fs1> = fieldvalue.

    fieldname = 'COUNT'.
    fieldvalue = <ls_context>-count.
    assign component fieldname of structure <dyn_wa> to <fs1>.
    <fs1> = fieldvalue.

    lv_date_iterator = datefrom.
    lv_date_end = dateto.

    while lv_date_iterator le lv_date_end.

      concatenate lv_date_iterator+6(2) '-' lv_date_iterator+4(2) '-' lv_date_iterator(4) into lv_dats_iterator.

      fieldname = lv_dats_iterator.

      read table lt_ctx_aggr with key context_name = <ls_context>-context_name event_date = lv_date_iterator assigning <ls_ctx_aggr>.

      if ( sy-subrc = 0 ) and ( <ls_ctx_aggr> is assigned ).

        fieldvalue = <ls_ctx_aggr>-count.

        unassign <ls_ctx_aggr>.
      else.
        fieldvalue = 0.
      endif.

      lv_date_iterator = lv_date_iterator + 1.

      assign component fieldname of structure <dyn_wa> to <fs1>.
      <fs1> = fieldvalue.

    endwhile.

    append <dyn_wa> to <dyn_table>.

  endloop.


  wa_layout-colwidth_optimize = 'X'.


  if ip_mode = 'false'.


    call function 'REUSE_ALV_GRID_DISPLAY'
      exporting
        it_fieldcat            = alv_fldcat
        i_callback_program     = sy-repid
        is_layout              = wa_layout
        i_callback_top_of_page = 'TOP_AGGR_S'
      tables
        t_outtab               = <dyn_table>.


  else.

    call function 'REUSE_ALV_GRID_DISPLAY'
      exporting
        it_fieldcat            = alv_fldcat
        i_callback_program     = sy-repid
        is_layout              = wa_layout
        i_callback_top_of_page = 'TOP_AGGR_A'
      tables
        t_outtab               = <dyn_table>.
  endif.

endform. " by_context_aggregated

"--------------------------------------- By context (detailed) -----------------------------------------

form by_context_detailed using ip_mode ip_context_name.

  types: begin of ty_lt_ctx_det_aggr_tt,
           event_date     type dats,
           event_time     type char8,
           context_name   type ac_context_name,
           alert_name     type ac_placeholder_name,
           rating         type char10,
           notif_active   type char1,
           metric_details type ac_placeholder_name,

         end of ty_lt_ctx_det_aggr_tt.

  data lt_ctx_det_aggr type standard table of ty_lt_ctx_det_aggr_tt.
  field-symbols <ls_ctx_det_aggr> like line of lt_ctx_det_aggr.

  data wa_layout  type slis_layout_alv.

  get run time field lv_exec_time_start.

  if ip_mode = 'false'.

    select event_date event_time context_name alert_name rating notif_active metric_details from zsmartmonitorlog into table lt_ctx_det_aggr
    where source = 'T'
    and suppressed = 'X'
    and event_date ge datefrom
    and event_date le dateto.

  else.

    select  event_date  event_time context_name alert_name rating notif_active metric_details from zsmartmonitorlog into table lt_ctx_det_aggr
    where source = 'T'
    and anomaly_detected = 'X'
    and event_date ge datefrom
    and event_date le dateto.

  endif. " if ip_mode = 'false'

  sort lt_ctx_det_aggr by event_date.

  if ip_context_name is not initial.

    delete lt_ctx_det_aggr where context_name <> ip_context_name.

  endif.

  fieldcatalog-fieldname   = 'EVENT_DATE'.
  fieldcatalog-seltext_m   = 'Date'.
  fieldcatalog-col_pos     = 1.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'EVENT_TIME'.
  fieldcatalog-seltext_m   = 'Time'.
  fieldcatalog-col_pos     = 2.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'CONTEXT_NAME'.
  fieldcatalog-seltext_m   = 'Object'.
  fieldcatalog-col_pos     = 3.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'ALERT_NAME'.
  fieldcatalog-seltext_m   = 'Alert'.
  fieldcatalog-col_pos     = 4.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'RATING'.
  fieldcatalog-seltext_m   = 'Rating'.
  fieldcatalog-col_pos     = 5.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'NOTIF_ACTIVE'.
  fieldcatalog-seltext_m   = 'Notification'.
  fieldcatalog-col_pos     = 6.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'METRIC_DETAILS'.
  fieldcatalog-seltext_l   = 'Metric details'.
  fieldcatalog-col_pos     = 7.
  fieldcatalog-outputlen = 250.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  wa_layout-colwidth_optimize = 'X'.

  if ip_mode = 'false'.

    call function 'REUSE_ALV_GRID_DISPLAY'
      exporting
        i_callback_program     = sy-repid
        it_fieldcat            = fieldcatalog[]
        is_layout              = wa_layout
        i_callback_top_of_page = 'TOP_AGGR_S'
      tables
        t_outtab               = lt_ctx_det_aggr.
  else.

    call function 'REUSE_ALV_GRID_DISPLAY'
      exporting
        i_callback_program     = sy-repid
        it_fieldcat            = fieldcatalog[]
        is_layout              = wa_layout
        i_callback_top_of_page = 'TOP_AGGR_A'
      tables
        t_outtab               = lt_ctx_det_aggr.

  endif.

  refresh fieldcatalog[].

endform.

"---------------------------------------------------- Top lists ----------------------------------------------------

form top_lists.

  get run time field lv_exec_time_start.

  " Top suppression

  select context_name alert_name from zsmartmonitorlog into table lt_alerts_by_context
  where source = 'T'
  and suppressed = 'X'
  and event_date ge datefrom
  and event_date le dateto.

  wa_alerts_by_context-count = '1'.

  modify lt_alerts_by_context from wa_alerts_by_context transporting count where count = '0' .

  loop at lt_alerts_by_context into wa_alerts_by_context.

    collect wa_alerts_by_context into lt_alerts_by_context_group_s.

  endloop.

  sort lt_alerts_by_context_group_s by count descending.

  describe table lt_alerts_by_context_group_s lines lv_records_counter.

  if lv_records_counter > 50.
    delete lt_alerts_by_context_group_s from 51 to lv_records_counter.
  endif.

  call function 'REUSE_ALV_BLOCK_LIST_INIT'
    exporting
      i_callback_program = sy-repid.

  fieldcatalog-fieldname   = 'CONTEXT_NAME'.
  fieldcatalog-seltext_m   = 'Object'.
  fieldcatalog-col_pos     = 1.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'ALERT_NAME'.
  fieldcatalog-seltext_m   = 'Alert'.
  fieldcatalog-emphasize = 'C500'.
  fieldcatalog-col_pos     = 2.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'COUNT'.
  fieldcatalog-seltext_m   = 'Suppressed'.
  fieldcatalog-col_pos     = 3.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  wa_layout-colwidth_optimize = 'X'.

  wa_events-name = 'TOP_OF_PAGE'.
  wa_events-form = 'TOP_SUPPRESSED'.
  append wa_events to lt_events.
  clear wa_events.

  call function 'REUSE_ALV_BLOCK_LIST_APPEND'
    exporting
      is_layout   = wa_layout
      it_fieldcat = fieldcatalog[]
      i_tabname   = ''
      it_events   = lt_events
    tables
      t_outtab    = lt_alerts_by_context_group_s.

  refresh fieldcatalog[].

  " Top approvals

  select context_name alert_name from zsmartmonitorlog into table lt_alerts_by_context
  where source = 'T'
  and anomaly_detected = 'X'
  and event_date ge datefrom
  and event_date le dateto.

  wa_alerts_by_context-count = '1'.

  modify lt_alerts_by_context from wa_alerts_by_context transporting count where count = '0' .

  loop at lt_alerts_by_context into wa_alerts_by_context.

    collect wa_alerts_by_context into lt_alerts_by_context_group_a.

  endloop.

  sort lt_alerts_by_context_group_a by count descending.

  describe table lt_alerts_by_context_group_a lines lv_records_counter.

  if lv_records_counter > 50.
    delete lt_alerts_by_context_group_a from 51 to lv_records_counter.
  endif.

  fieldcatalog-fieldname   = 'CONTEXT_NAME'.
  fieldcatalog-seltext_m   = 'Object'.
  fieldcatalog-col_pos     = 1.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.


  fieldcatalog-fieldname   = 'ALERT_NAME'.
  fieldcatalog-seltext_m   = 'Alert'.
  fieldcatalog-emphasize = 'C500'.
  fieldcatalog-col_pos     = 2.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  fieldcatalog-fieldname   = 'COUNT'.
  fieldcatalog-seltext_m   = 'Approved'.
  fieldcatalog-col_pos     = 3.
  append fieldcatalog to fieldcatalog.
  clear  fieldcatalog.

  wa_layout-colwidth_optimize = 'X'.

  wa_events-name = 'TOP_OF_PAGE'.
  wa_events-form = 'TOP_APPROVED'.
  append wa_events to lt_events.
  clear wa_events.


  call function 'REUSE_ALV_BLOCK_LIST_APPEND'
    exporting
      is_layout   = wa_layout
      it_fieldcat = fieldcatalog[]
      i_tabname   = ''
      it_events   = lt_events
    tables
      t_outtab    = lt_alerts_by_context_group_a.

  call function 'REUSE_ALV_BLOCK_LIST_DISPLAY'.

endform.

form top_of_page.

  data: it_listheader type slis_t_listheader,
        wa_listheader type slis_listheader.

  data lv_processing_time_char type char20.

  data lv_digit_char_1 type char10.
  data lv_digit_char_2 type char10.

  wa_listheader-typ  = 'H'.
  wa_listheader-info ='Machine Learning Alerts Analysis Summary'.
  append wa_listheader to it_listheader.
  clear wa_listheader.

  wa_listheader-typ  = 'A'.

  concatenate 'Period' ':' into wa_listheader-info.
  concatenate wa_listheader-info datefrom+6(2) into wa_listheader-info separated by space.
  concatenate wa_listheader-info '.' datefrom+4(2) '.' datefrom(4) into wa_listheader-info.
  concatenate wa_listheader-info '-' into wa_listheader-info separated by space.
  concatenate wa_listheader-info dateto+6(2) into wa_listheader-info separated by space.
  concatenate wa_listheader-info '.' dateto+4(2) '.' dateto(4) into wa_listheader-info.

  append wa_listheader to it_listheader.
  clear wa_listheader.

  lv_digit_char_1 = lv_suppressed_count_total.
  lv_digit_char_2 = lv_approved_count_total.
  condense lv_digit_char_1.
  condense lv_digit_char_2.
  concatenate 'Total suppressed (false):' lv_digit_char_1 '/ Total approved (true):' lv_digit_char_2 into wa_listheader-info separated by space.
  wa_listheader-typ  = 'A'.
  append wa_listheader to it_listheader.
  clear wa_listheader.

  lv_digit_char_1 = ( lv_suppressed_count_total / ( lv_suppressed_count_total + lv_approved_count_total ) ) * 100.
  lv_digit_char_2 = ( lv_approved_count_total / ( lv_suppressed_count_total + lv_approved_count_total ) ) * 100.
  condense lv_digit_char_1.
  condense lv_digit_char_2.
  concatenate 'False alerts (%):' lv_digit_char_1+0(6) '/ True alerts (%):' lv_digit_char_2+0(6) into wa_listheader-info separated by space.
  wa_listheader-typ  = 'A'.
  append wa_listheader to it_listheader.
  clear wa_listheader.

  get run time field lv_exec_time_end.
  lv_processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

  wa_listheader-typ  = 'A'.
  lv_processing_time_char = lv_processing_time.
  condense lv_processing_time_char.
  concatenate 'Processing time:' lv_processing_time_char 'ms' into wa_listheader-info separated by space.

  append wa_listheader to it_listheader.
  clear wa_listheader.

  call function 'REUSE_ALV_COMMENTARY_WRITE'
    exporting
      it_list_commentary = it_listheader.

endform.                    "top_of_page

form top_aggr_a.

  data: it_listheader type slis_t_listheader,
        wa_listheader type slis_listheader.

  data lv_processing_time_char type char20.


  data lv_digit_char_1 type char10.
  data lv_digit_char_2 type char10.

  wa_listheader-typ  = 'H'.

  wa_listheader-info ='Machine Learning Alerts: true alerts in detail'.

  append wa_listheader to it_listheader.
  clear wa_listheader.

  wa_listheader-typ  = 'A'.

  concatenate 'Period' ':' into wa_listheader-info.
  concatenate wa_listheader-info datefrom+6(2) into wa_listheader-info separated by space.
  concatenate wa_listheader-info '.' datefrom+4(2) '.' datefrom(4) into wa_listheader-info.
  concatenate wa_listheader-info '-' into wa_listheader-info separated by space.
  concatenate wa_listheader-info dateto+6(2) into wa_listheader-info separated by space.
  concatenate wa_listheader-info '.' dateto+4(2) '.' dateto(4) into wa_listheader-info.

  append wa_listheader to it_listheader.
  clear wa_listheader.

  get run time field lv_exec_time_end.
  lv_processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

  wa_listheader-typ  = 'A'.
  lv_processing_time_char = lv_processing_time.
  condense lv_processing_time_char.
  concatenate 'Processing time:' lv_processing_time_char 'ms' into wa_listheader-info separated by space.

  append wa_listheader to it_listheader.
  clear wa_listheader.

  call function 'REUSE_ALV_COMMENTARY_WRITE'
    exporting
      it_list_commentary = it_listheader.

endform.

form top_aggr_s.

  data: it_listheader type slis_t_listheader,
        wa_listheader type slis_listheader.

  data lv_processing_time_char type char20.


  data lv_digit_char_1 type char10.
  data lv_digit_char_2 type char10.

  wa_listheader-typ  = 'H'.

  wa_listheader-info ='Machine Learning Alerts: false alerts in detail'.


  append wa_listheader to it_listheader.
  clear wa_listheader.

  wa_listheader-typ  = 'A'.

  concatenate 'Period' ':' into wa_listheader-info.
  concatenate wa_listheader-info datefrom+6(2) into wa_listheader-info separated by space.
  concatenate wa_listheader-info '.' datefrom+4(2) '.' datefrom(4) into wa_listheader-info.
  concatenate wa_listheader-info '-' into wa_listheader-info separated by space.
  concatenate wa_listheader-info dateto+6(2) into wa_listheader-info separated by space.
  concatenate wa_listheader-info '.' dateto+4(2) '.' dateto(4) into wa_listheader-info.

  append wa_listheader to it_listheader.
  clear wa_listheader.

  get run time field lv_exec_time_end.
  lv_processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

  wa_listheader-typ  = 'A'.
  lv_processing_time_char = lv_processing_time.
  condense lv_processing_time_char.
  concatenate 'Processing time:' lv_processing_time_char 'ms' into wa_listheader-info separated by space.

  append wa_listheader to it_listheader.
  clear wa_listheader.

  call function 'REUSE_ALV_COMMENTARY_WRITE'
    exporting
      it_list_commentary = it_listheader.

endform.


form top_suppressed.

  data lv_processing_time_char type char10.

  get run time field lv_exec_time_end.

  lv_processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

  lv_processing_time_char = lv_processing_time.

  condense lv_processing_time_char.

  write:/ 'Processing time (ms): ', lv_processing_time_char.
  write:/.
  write:/ 'Top 50 alerts by suppression (false alerts)' color 6.
endform.


form top_approved.

  data lv_processing_time_char type char10.

  get run time field lv_exec_time_end.

  lv_processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

  lv_processing_time_char = lv_processing_time.

  condense lv_processing_time_char.

  write:/ 'Processing time (ms): ', lv_processing_time_char.
  write:/.
  write:/ 'Top 50 alerts by approvals (true alerts)' color 5.
endform.


form top_config.

  data: it_listheader type slis_t_listheader,
        wa_listheader type slis_listheader.

  data lv_processing_time_char type char20.


  data lv_digit_char_1 type char10.
  data lv_digit_char_2 type char10.

  wa_listheader-typ  = 'H'.

  wa_listheader-info ='Machine Learning Alerts: alerts with ZSMARTALERTS reaction'.


  append wa_listheader to it_listheader.
  clear wa_listheader.



  get run time field lv_exec_time_end.
  lv_processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

  wa_listheader-typ  = 'A'.
  lv_processing_time_char = lv_processing_time.
  condense lv_processing_time_char.
  concatenate 'Processing time:' lv_processing_time_char 'ms' into wa_listheader-info separated by space.

  append wa_listheader to it_listheader.
  clear wa_listheader.

  call function 'REUSE_ALV_COMMENTARY_WRITE'
    exporting
      it_list_commentary = it_listheader.

endform.