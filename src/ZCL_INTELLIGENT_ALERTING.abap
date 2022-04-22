class ZCL_INTELLIGENT_ALERTING definition
  public
  final
  create public .

public section.

  interfaces IF_ALERT_NOTIF_MULTI_CONF .
  interfaces IF_ALERT_REACTION .
  interfaces IF_BADI_INTERFACE .

  types:
    BEGIN OF st_my_alert_data,
        alert_name          TYPE string,
        managed_object_name TYPE string,
        managed_object_type TYPE string,
        category            TYPE string,
        severity            TYPE string,
        timestamp           TYPE string,
        rating              TYPE string,
        status              TYPE string,
        alert_id            TYPE string,
        visited             TYPE string,
        xml                 TYPE xstring,
        metricpath          TYPE string,
        reasonforclosure    TYPE string,
      END OF st_my_alert_data .
  types:
    BEGIN OF st_my_sub_object_data,
        name             TYPE string,
        event_id         TYPE string,
        obj_type         TYPE string,
        alert_id         TYPE string,  "contains the alert id from the root
        parent_id        TYPE string,  "contains immediate parent
        rating           TYPE string,
        text             TYPE string,
        visited          TYPE string,
        value            TYPE string,
        timestamp        TYPE string,
        metricpath       TYPE string,
        reasonforclosure TYPE string,

      END OF st_my_sub_object_data .
  types:
    tt_my_alert_data TYPE STANDARD TABLE OF st_my_alert_data WITH KEY alert_id .
  types:
    tt_my_sub_objects TYPE STANDARD TABLE OF st_my_sub_object_data .
  types:
    BEGIN OF st_abnormal_pair,
        context_name       TYPE ac_context_name,
        mname              TYPE ac_name,
        metric_short_text  TYPE ac_text_100,
        anomaly_date       TYPE datum,
        anomaly_time       TYPE char8,
        abnormality_rating TYPE char8,
    end of st_abnormal_pair .
  types:
    tt_abnormal_pair TYPE STANDARD TABLE OF st_abnormal_pair .

  class-data GT_EVENTS type TT_MY_SUB_OBJECTS .

  class-methods CALC_DURATION_BTW_TIMESTAMPS
    importing
      !IP_TIMESTAMP_1 type E2EA_TIMESTAMP
      !IP_TIMESTAMP_2 type E2EA_TIMESTAMP
    exporting
      !EP_DURATION type INTEGER .
  class-methods LOG_SMARTMON_RECORD
    importing
      !IP_MSGTY type SYMSGTY
      !IP_LOG_RECORD_TEXT type STRING .
  class-methods IMPORT_ANOMALIES_BY_TIMESTAMP
    importing
      !IP_TIME type UZEIT
      !IP_DATE type DATUM
    exporting
      !ET_ABNORMAL_PAIR type TT_ABNORMAL_PAIR .
protected section.
private section.

  methods EXTRACT_SUB_OBJECTS
    importing
      !IPO_OBJECT type ref to IF_ALERT_CONSM_OBJECT
      !IPV_ALERT_ID type STRING
      !IPV_PARENT_ID type STRING .
  class-methods INTERACT_WITH_MLE
    importing
      !IP_DESTINATION type RFCDEST
      !IP_PARAMLINE type CHAR1024
    exporting
      !EV_JSON_RESPONSE type STRING .
ENDCLASS.



CLASS ZCL_INTELLIGENT_ALERTING IMPLEMENTATION.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Static Public Method ZCL_INTELLIGENT_ALERTING=>CALC_DURATION_BTW_TIMESTAMPS
* +-------------------------------------------------------------------------------------------------+
* | [--->] IP_TIMESTAMP_1                 TYPE        E2EA_TIMESTAMP
* | [--->] IP_TIMESTAMP_2                 TYPE        E2EA_TIMESTAMP
* | [<---] EP_DURATION                    TYPE        INTEGER
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD calc_duration_btw_timestamps.


    DATA lv_timestamp_1   TYPE c LENGTH 14.
    DATA lv_timestamp_2   TYPE c LENGTH 14.


    DATA: lv_date_1      TYPE d,
          lv_date_2      TYPE d,
          lv_time_1      TYPE t,
          lv_time_2      TYPE t.


    if ( ip_timestamp_1 = 0 ) OR ( ip_timestamp_2 = 0 ).

      ep_duration = 0.
      return.

    endif.

    lv_timestamp_1 = ip_timestamp_1.
    lv_timestamp_2 = ip_timestamp_2.

    IF ip_timestamp_1 < ip_timestamp_2.
      lv_date_2       = lv_timestamp_1(8).
      lv_time_2       = lv_timestamp_1+8(6).
      lv_date_1       = lv_timestamp_2(8).
      lv_time_1       = lv_timestamp_2+8(6).
    ELSE.
      lv_date_1       = lv_timestamp_1(8).
      lv_time_1       = lv_timestamp_1+8(6).
      lv_date_2       = lv_timestamp_2(8).
      lv_time_2       = lv_timestamp_2+8(6).
    ENDIF.

    ep_duration = ( ( ( lv_date_1 - lv_date_2 ) * 86400
                 + ( lv_time_1 - lv_time_2 ) ) ).

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method ZCL_INTELLIGENT_ALERTING->EXTRACT_SUB_OBJECTS
* +-------------------------------------------------------------------------------------------------+
* | [--->] IPO_OBJECT                     TYPE REF TO IF_ALERT_CONSM_OBJECT
* | [--->] IPV_ALERT_ID                   TYPE        STRING
* | [--->] IPV_PARENT_ID                  TYPE        STRING
* +--------------------------------------------------------------------------------------</SIGNATURE>
METHOD EXTRACT_SUB_OBJECTS.

  DATA ls_extracted_sub_object TYPE CLASS_APP_METRIC=>st_my_sub_object_data.
  DATA ls_sub_object_temp TYPE  Class_app_metric=>st_my_sub_object_data.
  DATA lt_sub_objects TYPE e2ea_t_alert_consm_object.
  DATA lo_sub_object TYPE REF TO if_alert_consm_object.

  "Code for cycle detection in event hierarchy
  READ TABLE gt_events INTO ls_sub_object_temp WITH KEY event_id = ipo_object->get_id( ).

  IF sy-subrc = 0.
    IF ls_sub_object_temp-visited = 'TRUE'.
      " we have a cycle in the event hierarchy
      " handle it according to your convenience.
      "This is an error or exception case.
    ENDIF.
  ENDIF.

  ls_extracted_sub_object-alert_id = ipv_alert_id.
  ls_extracted_sub_object-event_id = ipo_object->get_id( ).
  ls_extracted_sub_object-obj_type = ipo_object->get_object_type( ).
  ls_extracted_sub_object-parent_id = ipv_parent_id.
  ls_extracted_sub_object-visited = 'TRUE'.
  ls_extracted_sub_object-value = ipo_object->get_value( ).
  ls_extracted_sub_object-timestamp = ipo_object->get_timestamp( ).
  ls_extracted_sub_object-metricpath = ipo_object->get_metric_path( ).
  ls_extracted_sub_object-reasonforclosure = ipo_object->get_reason_for_closure( ).
*  IF ipo_object->get_metric_path( ) IS INITIAL.
    ls_extracted_sub_object-name = ipo_object->get_name( ).
*  ELSE.
*    ls_extracted_sub_object-name = ipo_object->get_metric_path( ).
*  ENDIF.

  ls_extracted_sub_object-rating = cl_alert_consm_utility=>get_domain_value_text(
    i_domain_name = cl_alert_consm_constants=>ac_domname_rating
    i_value = ipo_object->get_rating( )
    ).

  ls_extracted_sub_object-text = ipo_object->get_text_value( ).

  APPEND ls_extracted_sub_object TO gt_events.

  IF ipo_object->get_object_type( ) = cl_alert_consm_constants=>ac_metric_consm_object.
    RETURN.
  ELSEIF ipo_object->get_object_type( ) = cl_alert_consm_constants=>ac_event_consm_object
    OR ipo_object->get_object_type( ) = cl_alert_consm_constants=>ac_metricgrp_consm_object.

    IF ipo_object->has_sub_objects( ) = abap_true.
      lt_sub_objects = ipo_object->get_sub_objects( ).
      LOOP AT lt_sub_objects INTO lo_sub_object.
        EXTRACT_SUB_OBJECTS( ipo_object = lo_sub_object
                             ipv_parent_id = ls_extracted_sub_object-event_id
                             ipv_alert_id = ipv_alert_id ).
      ENDLOOP.

    ENDIF.
  ENDIF.

ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_INTELLIGENT_ALERTING->IF_ALERT_NOTIF_MULTI_CONF~GET_INCI_CONF_FOR_ALERT
* +-------------------------------------------------------------------------------------------------+
* | [--->] IO_ALERT_OBJECT                TYPE REF TO CL_ALERT_CONSM_OBJECT
* | [--->] IV_XML                         TYPE        AC_XSTRING
* | [--->] IV_FILTER_VALUE                TYPE        AC_REACTION_ID(optional)
* | [--->] IV_INTRO_LONGTEXT              TYPE        STRING(optional)
* | [--->] IV_CONFIG_SUBJECT              TYPE        STRING(optional)
* | [--->] IV_DEFAULT_SHORT_TEXT          TYPE        STRING(optional)
* | [--->] IV_DEFAULT_LONG_TEXT           TYPE        STRING(optional)
* | [<-->] ET_ATTACHMENTS                 TYPE        DSMOPTMSGATTACH
* | [<-->] E_INCIDENT_TEXT                TYPE        STRING
* | [<-->] E_INCIDENT_SUBJECT             TYPE        TEXT60
* | [<-->] E_INCIDENT_COMPONENT           TYPE        UFPS_POSID
* | [<-->] E_CRM_TRANSACTION_TYPE         TYPE        CRMT_PROCESS_TYPE
* | [<-->] E_CATEGORYID                   TYPE        CRM_ERMS_CAT_CA_ID
* | [<-->] E_ASPECTID                     TYPE        CRM_ERMS_CAT_AS_ID
* | [<-->] E_PROCESSOR                    TYPE        SYUNAME
* +--------------------------------------------------------------------------------------</SIGNATURE>
  method IF_ALERT_NOTIF_MULTI_CONF~GET_INCI_CONF_FOR_ALERT.
  endmethod.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_INTELLIGENT_ALERTING->IF_ALERT_NOTIF_MULTI_CONF~GET_NOTIF_CONF_FOR_ALERT
* +-------------------------------------------------------------------------------------------------+
* | [--->] IO_ALERT_OBJECT                TYPE REF TO CL_ALERT_CONSM_OBJECT
* | [--->] IV_XML                         TYPE        AC_XSTRING
* | [--->] IT_STD_NOTIF_CONFIG            TYPE        STRING(optional)
* | [--->] IV_CONFIG_SUBJECT              TYPE        STRING(optional)
* | [--->] IV_INTRO_LONGTEXT              TYPE        STRING(optional)
* | [--->] IV_FILTER_VALUE                TYPE        AC_REACTION_ID(optional)
* | [--->] IV_DEFAULT_SUBJECT             TYPE        STRING(optional)
* | [--->] IV_DEFAULT_LONG_TEXT           TYPE        STRING(optional)
* | [<-->] EP_EMAIL_TEXT_TYPE             TYPE        SO_OBJ_TP
* | [<-->] EP_EMAIL_TEXT                  TYPE        STRING
* | [<-->] EP_SMS_TEXT                    TYPE        STRING
* | [<-->] EP_EMAIL_SUBJECT               TYPE        STRING
* | [<-->] ET_ATTACHMENTS                 TYPE        CL_DSWP_NA_NOTIF_ADMIN=>DSWPT_NA_EMAIL_ATTACHMENT
* | [<-->] EV_SENDER                      TYPE        UNAME
* | [<-->] EP_APPEND_ATTACHMENT           TYPE        BOOLEAN
* | [<-->] ET_EMAIL_IDS                   TYPE        CL_DSWP_NM_TYPES=>DSWPT_NM_CONTACTS
* | [<-->] ET_SMS_NUMBERS                 TYPE        CL_DSWP_NM_TYPES=>DSWPT_NM_CONTACTS
* | [<-->] EP_OVERWRITE_EMAIL_SMS         TYPE        BOOLEAN
* | [<-->] EP_SUPRESS_SMS                 TYPE        BOOLEAN(optional)
* | [<-->] EP_SUPRESS_EMAIL               TYPE        BOOLEAN(optional)
* | [<-->] EV_SUPRESS_ATTACHMENT          TYPE        BOOLEAN(optional)
* +--------------------------------------------------------------------------------------</SIGNATURE>
  method if_alert_notif_multi_conf~get_notif_conf_for_alert.

    data lv_alert_guid type ac_guid.
    data lv_suppression_status type char1.
    data lv_suppression_mode type char1.
    data lv_subject_marker type char258.

    select single value from zsmartmonparam into lv_suppression_mode
        where param = 'FALSE_ALERT_ACTION'.

    condense lv_suppression_mode.

    if ( lv_suppression_mode <> '' ).

      lv_alert_guid = io_alert_object->if_alert_consm_object~get_id( ).

      if ( lv_alert_guid is not initial ).

        select suppressed into lv_suppression_status from zsmartmonitorlog  up to 1 rows
          where alert_guid = lv_alert_guid
            order by record_id descending.

        endselect.

        if lv_suppression_status = 'X'.

          case lv_suppression_mode.

            when 'M'.

              select single value from zsmartmonparam into lv_subject_marker
                   where param = 'EMAIL_SUBJECT_MARKER'.

              condense lv_subject_marker.

              if ( lv_subject_marker <> '' ).

                concatenate lv_subject_marker iv_default_subject into ep_email_subject separated by space.

              endif. " IF ( lv_subject_marker <> '' )

            when 'S'.

              ep_supress_email = 'X'.

          endcase. " CASE lv_suppression_mode

          update zsmartmonitorlog set email_action = lv_suppression_mode where alert_guid = lv_alert_guid.

        endif. " IF lv_suppression_status = 'X'


      endif. " IF ( lv_alert_guid IS NOT INITIAL )

    endif. " IF ( lv_suppression_mode <> '' )


  endmethod.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_INTELLIGENT_ALERTING->IF_ALERT_REACTION~IS_AUTO_REACTION
* +-------------------------------------------------------------------------------------------------+
* | [<-->] CV_FLAG                        TYPE        ABAP_BOOL
* +--------------------------------------------------------------------------------------</SIGNATURE>
  method IF_ALERT_REACTION~IS_AUTO_REACTION.
  endmethod.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_INTELLIGENT_ALERTING->IF_ALERT_REACTION~REACT_TO_ALERTS
* +-------------------------------------------------------------------------------------------------+
* | [--->] IPT_ALERTS                     TYPE        E2EA_T_ALERT_CONSM_OBJECT
* | [--->] IP_XML                         TYPE        AC_XSTRING
* | [--->] IP_FILTER_VAL                  TYPE        AC_REACTION_ID(optional)
* +--------------------------------------------------------------------------------------</SIGNATURE>
  method if_alert_reaction~react_to_alerts.

    data: lv_alert_guid          type ac_guid,
          lv_object_type         type char10,
          lv_context_id          type ac_guid,
          lv_metric_id           type ac_guid,
          lv_alert_timestamp_utc type e2ea_timestamp.

    data wa_zsmartmonitorlog type zsmartmonitorlog.

    data lv_json_response type string.
    data lv_paramline type char1024.
    data lv_suppression_mode type char258.

    field-symbols <lfs_alert> like line of ipt_alerts.

    " Structures for pythia response

    data: lr_json  type ref to /ui2/cl_json.
    types: begin of ty_pythia_response_tt,
             code  type char128,
             value type char128,
           end of ty_pythia_response_tt.

    data lt_pythia_response type standard table of ty_pythia_response_tt.
    data ls_pythia_response type ty_pythia_response_tt.

    data lv_pythia_abnormality type char1.

    data lv_record_id type i.

    data lv_last_run_id type i.
    data lv_next_run_id type i.

    " Structures for alerts confirmations

    data lo_acc type ref to cl_alert_consumer_connector.
    data lt_suppressed_alerts type e2ea_t_guid.

    field-symbols <ls_suppressed_alerts> like line of lt_suppressed_alerts.

    data lv_suppression_challenge type c1.
    data lv_supp_chall_met_list type string.
    data lv_noreply_challenge type c1.
    data lv_noreply_detected type c1.

    " Structures for thresholds

    data lt_acmetricdir type standard table of acmetricdir.
    field-symbols <ls_acmetricdir> like line of lt_acmetricdir.
    data lv_db_tab_name type ddobjname.

    data lt_sub_objects type e2ea_t_alert_consm_object.
    data lo_sub_object type ref to if_alert_consm_object.
    data ls_extracted_alert type st_my_alert_data.
    data lt_extracted_alerts type tt_my_alert_data.

    field-symbols <gt_events> like line of gt_events.

    data lv_rule_type type ac_rule_types.

    data lv_greentoyellow type ac_green_to_yellow.
    data lv_yellowtored type ac_yellow_to_red.
    data lv_greentored type ac_green_to_red.

    data lv_greentoyellow_char type char10.
    data lv_yellowtored_char type char10.
    data lv_greentored_char type char10.


    data lv_metric_line type string.


    data lv_time_app type uzeit.
    data lv_date_app type datum.
    data lv_alert_timestamp_app type e2ea_timestamp.
    data lv_timezone type timezone.

    data lv_timestamp_char type char20.
    data lv_timestamp type e2ea_timestamp.
    data lv_a_events_count type i.


    data lv_short_text type ac_text_100.
    data lv_context_name type ac_context_name.

    data lv_metric_tech_name type ac_name.

    data lv_exec_time_start type i.
    data lv_exec_time_end type i.

    data lv_log_record_text type string.

    get run time field lv_exec_time_start.

    lv_log_record_text = 'Opening alerts evaluation sequence'.
    log_smartmon_record(
      ip_msgty  = 'I'
      ip_log_record_text = lv_log_record_text ).

    loop at ipt_alerts assigning <lfs_alert>.

      " Getting a GUID for an Alert

      lv_object_type = <lfs_alert>->get_object_type( ).

      if lv_object_type = 'A'.

        lv_alert_guid = <lfs_alert>->get_id( ).

        concatenate 'Analyzing alert ' lv_alert_guid into lv_log_record_text separated by space.

        log_smartmon_record(
           ip_msgty  = 'I'
           ip_log_record_text = lv_log_record_text ).

        " Getting sub-objects list for metric)id

        if <lfs_alert>->has_sub_objects( ) = abap_true.

          lt_sub_objects = <lfs_alert>->get_sub_objects( ).

          loop at lt_sub_objects into lo_sub_object.

            extract_sub_objects( ipo_object = lo_sub_object

            ipv_parent_id = <lfs_alert>->get_id( )

            ipv_alert_id = ls_extracted_alert-alert_id ).

            append ls_extracted_alert to lt_extracted_alerts.
            clear ls_extracted_alert.

          endloop. " LOOP AT lt_sub_objects INTO lo_sub_object.

        endif. "IF <lfs_alert>->has_sub_objects( ) = abap_true.


        " Preparing a unique key for a record in zsmartmonitorlog

        select run_id into lv_last_run_id from zsmartmonitorlog up to 1 rows
          order by run_id descending.

          lv_next_run_id = lv_last_run_id + 1.

        endselect.

        " We loop through all alert metrics with a non-green rating.
        " If at least one anomaly found, we conclude, that alert is RELIABLE

        lv_suppression_challenge = ''.

        loop at gt_events assigning <gt_events>.

          if ( <gt_events>-obj_type = 'M' ) and ( <gt_events>-rating <> 'Green').

            lv_metric_id = <gt_events>-event_id.

            concatenate  'Found metric_id' lv_metric_id 'with not green rating ' into lv_log_record_text separated by space.

            log_smartmon_record(
               ip_msgty  = 'I'
               ip_log_record_text = lv_log_record_text ).

            select single context_id  start_timestamp
              into ( lv_context_id, lv_alert_timestamp_utc ) from e2ea_alertgroup
              where algroup_id = lv_alert_guid.

            " Converting alert timestamp from UTC timestamp into app server time

            call function 'GET_SYSTEM_TIMEZONE'
              importing
                timezone = lv_timezone.

            convert time stamp lv_alert_timestamp_utc time zone lv_timezone into date lv_date_app time lv_time_app.

            convert date  lv_date_app time lv_time_app into time stamp lv_alert_timestamp_app time zone 'UTC'.

            if ( lv_context_id is not initial ) and ( lv_metric_id is not initial ).

              " Preparing initial alert data for writing to ZSMARTMONITORLOG

              " Getting latest ID from ZSMARTMONITORLOG

              select record_id into lv_record_id from zsmartmonitorlog up to 1 rows
              order by record_id descending.

                lv_record_id = lv_record_id + 1.

              endselect.


              " Getting technical name of the metric

              clear lv_rule_type.
              clear lv_metric_tech_name.

              select single name rule_type from acmetricdir into ( lv_metric_tech_name, lv_rule_type )
                where context_id  = lv_context_id
                and event_type_id = lv_metric_id
                and ac_variant = 'A'.

              clear wa_zsmartmonitorlog.

              wa_zsmartmonitorlog-alert_name = <lfs_alert>->get_name( ).
              wa_zsmartmonitorlog-record_id = lv_record_id.
              wa_zsmartmonitorlog-run_id = lv_next_run_id.
              wa_zsmartmonitorlog-source = 'T'.
              wa_zsmartmonitorlog-alert_guid = lv_alert_guid.
              wa_zsmartmonitorlog-context_id = lv_context_id.
              wa_zsmartmonitorlog-metric_id = lv_metric_id.
              wa_zsmartmonitorlog-start_timestamp = lv_alert_timestamp_app.
              wa_zsmartmonitorlog-update_timestamp = lv_alert_timestamp_app.
              wa_zsmartmonitorlog-event_date = lv_date_app.
              wa_zsmartmonitorlog-rating = <gt_events>-rating.
              wa_zsmartmonitorlog-metric_tname = lv_metric_tech_name.
              wa_zsmartmonitorlog-tech_scenario = <lfs_alert>->get_technical_scenario( ).
              wa_zsmartmonitorlog-context_type = <lfs_alert>->get_managed_object_type( ).

              concatenate lv_time_app(2) lv_time_app+2(2) lv_time_app+4(2)
                into wa_zsmartmonitorlog-event_time separated by ':'.

              " Preparing information on notifications

              data lv_alert_type_id type string.
              data lv_notification_mode type char1.

              lv_alert_type_id = <lfs_alert>->get_alert_type_id( ).

              select single notification_mode from acnotifydir into lv_notification_mode
                where context_id  = lv_context_id
                and repository_id = lv_alert_type_id
                and ac_variant    = 'A'
                and workmode_id = ''
                and disabled <> 'X'.

              " Checking if notification record was found or not

              if ( sy-subrc = 0 ). " Record found

                " acnotifydir-notification_mode values mapping:
                " E - Email --> E
                " S - SMS --> S
                " empty - Email and SMS --> X

                if ( lv_notification_mode = '' ).

                  wa_zsmartmonitorlog-notif_active = 'X'.

                else.
                  wa_zsmartmonitorlog-notif_active = lv_notification_mode.

                endif.

              else. " Record not found

                wa_zsmartmonitorlog-notif_active = ''.

              endif.

              " Picking up context name and metric name

              select single context_name into lv_context_name from v_acentrypoints
                    where context_id = lv_context_id.

              if ( sy-subrc = 0 ).
                wa_zsmartmonitorlog-context_name = lv_context_name.
              endif.

              select single short_text into lv_short_text from acmetricdirt
                  where context_id = lv_context_id
                  and event_type_id = lv_metric_id
                  and langu = 'E'.


*              IF ( sy-subrc = 0 ).
*                wa_zsmartmonitorlog-metric_name = lv_short_text.
*              ELSE.
*                wa_zsmartmonitorlog-metric_name = 'Not available'.
*              ENDIF.

              " Checking if anomaly was not detected earlier by A events

              select count(*) into lv_a_events_count from zsmartmonitorlog
                where context_name = wa_zsmartmonitorlog-context_name
                "AND metric_tname = wa_zsmartmonitorlog-metric_tname
                and metric_tname = lv_metric_tech_name
                and source = 'A'
                and suppressed <> 'X'
                and run_id < lv_next_run_id.

              if ( lv_a_events_count <> 0 ).

                wa_zsmartmonitorlog-collision = 'X'.


                concatenate 'Collision found for metric' lv_metric_id  into lv_log_record_text separated by space.
                log_smartmon_record(
                   ip_msgty  = 'I'
                   ip_log_record_text = lv_log_record_text ).

              endif." IF ( lv_a_events_count = 0 ).

              " ----------------------- Thresholds identification start ------------------

              clear lv_greentoyellow.
              clear lv_greentored.
              clear lv_yellowtored.
              clear lv_greentoyellow_char.
              clear lv_greentored_char.
              clear lv_yellowtored_char.

              case lv_rule_type.

                when 'TEXTTHRESHOLD'.
                  lv_db_tab_name = 'ACTEXTTHRESHOLD'.
                when 'SIMPLETHRESHOLD'.
                  lv_db_tab_name = 'ACSIMPLETHRESDIR'.
                when 'REGEX'.
                  lv_db_tab_name = 'ACREGEXDIR'.
                when 'RANGE_THRESHOLD'.
                  lv_db_tab_name = 'ACRANGETHRESHDIR'.
                when 'PERCENTAGETHRESHOLD'.
                  lv_db_tab_name = 'ACPCTHRESDIR'.
                when 'NUMERIC_GR'.
                  lv_db_tab_name = 'ACNUMERIC_G_R'.
                when 'NUMERIC_GYR'.
                  lv_db_tab_name = 'ACNUMERIC_GYR'.
                when 'DELTA_THRESHOLD'.
                  lv_db_tab_name = 'ACDELTATHRESH'.
                when 'COUNTER_THRESHOLD'.
                  lv_db_tab_name = 'ACCOUNTERTHRESH'.
                when 'BEST_OF_LAST_N'.
                  lv_db_tab_name = 'ACBESTOFLASTN'.
                when others.
                  clear lv_db_tab_name.
              endcase.

              " GREENTOYELLOW and YELLOWTORED threshold types

              if lv_db_tab_name is not initial.

                " Validation that  greentoyellow field exist in a table     |
                call function 'DDIF_FIELDINFO_GET'
                  exporting
                    tabname        = lv_db_tab_name
                    fieldname      = 'greentoyellow'
                    langu          = sy-langu
                  exceptions
                    not_found      = 1
                    internal_error = 2
                    others         = 3.

                if sy-subrc = 0.


                  " Validation that yellowtored field exist in a table
                  call function 'DDIF_FIELDINFO_GET'
                    exporting
                      tabname        = lv_db_tab_name
                      fieldname      = 'yellowtored'
                      langu          = sy-langu
                    exceptions
                      not_found      = 1
                      internal_error = 2
                      others         = 3.

                  if sy-subrc = 0.

                    select single greentoyellow yellowtored from (lv_db_tab_name) into (lv_greentoyellow, lv_yellowtored)
                      where context_id = lv_context_id
                      and event_type_id = lv_metric_id
                      and ac_variant = 'A'.

                  endif. " IF sy-subrc = 0
                endif. " IF sy-subrc = 0

                " GREENTORED threshold types

                " Validation that  greentored field exist in a table     |
                call function 'DDIF_FIELDINFO_GET'
                  exporting
                    tabname        = lv_db_tab_name
                    fieldname      = 'greentored'
                    langu          = sy-langu
                  exceptions
                    not_found      = 1
                    internal_error = 2
                    others         = 3.

                if sy-subrc = 0.

                  select single greentored from (lv_db_tab_name) into lv_greentored
                    where context_id = lv_context_id
                    and event_type_id = lv_metric_id
                    and ac_variant = 'A'.

                endif. " IF sy-subrc = 0

              endif. " IF lv_db_tab_name IS NOT INITIAL


              clear lv_metric_line.
              condense <gt_events>-value.

              "CONCATENATE  lv_short_text ':' <gt_events>-rating '/' <gt_events>-value INTO lv_metric_line.

              concatenate  lv_short_text '/' lv_metric_tech_name ':' <gt_events>-rating '/' <gt_events>-value into lv_metric_line.


              if ( lv_greentored is not initial ).

                lv_greentored_char = lv_greentored.
                condense lv_greentored_char.

                concatenate lv_metric_line '/G2R:' lv_greentored_char into lv_metric_line.

              endif. " IF ( lv_greentored IS NOT INITIAL )

              if ( ( lv_greentoyellow is not initial ) and ( lv_yellowtored is not initial ) ).

                lv_greentoyellow_char = lv_greentoyellow.
                condense lv_greentoyellow_char.

                lv_yellowtored_char = lv_yellowtored.
                condense lv_yellowtored_char.

                concatenate lv_metric_line '/G2Y:' lv_greentoyellow_char '/Y2R:' lv_yellowtored_char into lv_metric_line.

              endif. " IF ( lv_greentored IS NOT INITIAL )


              "CONCATENATE lv_metric_line '/' lv_metric_tech_name INTO lv_metric_line.

              if ( wa_zsmartmonitorlog-collision  = 'X' ).

                concatenate lv_metric_line '/A-Collision'  into lv_metric_line.

              endif.

              " ----------------------- Evaluating anomaly status ------------------

              concatenate  'Pythia call: context_name =' lv_context_name 'metric_name=' lv_metric_tech_name  into lv_log_record_text separated by space.
              log_smartmon_record(
                 ip_msgty  = 'I'
                 ip_log_record_text = lv_log_record_text ).

              " Preparing parameters line to call Pythia service by name

              concatenate 'mode=name&context_name=' lv_context_name '&metric_name=' lv_metric_tech_name into lv_paramline.

              " Calling Pythia service via private method

              interact_with_mle(
                exporting
                 ip_destination = 'ZSMARTALERT'
                 ip_paramline = lv_paramline
                importing
                 ev_json_response = lv_json_response ).

*              CALL FUNCTION 'ZSMARTMON_TOUCH_MLE'
*                EXPORTING
*                  iv_destination   = 'ZSMARTALERT'
*                  iv_paramline     = lv_paramline
*                IMPORTING
*                  ev_json_response = lv_json_response.



              " -------- RANDOMIZER FOR TESTS START -------

*              DATA lv_random_number TYPE int4.
*
*              CALL FUNCTION 'QF05_RANDOM_INTEGER'
*                EXPORTING
*                  ran_int_max   = 1
*                  ran_int_min   = 0
*                IMPORTING
*                  ran_int       = lv_random_number
*                EXCEPTIONS
*                  invalid_input = 1
*                  OTHERS        = 2.
*
*              IF ( lv_random_number = 0 ).
*                lv_json_response = '{"code":0,"value":"0"}'.
*              ELSE.
*                lv_json_response = '{"code":0,"value":"1"}'.
*              ENDIF.

              " -------- RANDOMIZER FOR TESTS END -------


              " De-serializing Pythia response

              create object lr_json.
              try .
                  lr_json->deserialize_int( exporting json = lv_json_response changing data = ls_pythia_response ).

                catch cx_sy_move_cast_error into data(lo_move_cast_error) .
              endtry.

              append ls_pythia_response to lt_pythia_response.

              concatenate 'Pythia response code is' ls_pythia_response-code 'and value is' ls_pythia_response-value into lv_log_record_text separated by space.

              log_smartmon_record(
                 ip_msgty  = 'I'
                 ip_log_record_text = lv_log_record_text ).

              " Processing only if Pythia returned meaningful value

              if ( ( ls_pythia_response is not initial ) and ( ls_pythia_response-code = 0 ) ).

                lv_noreply_challenge = ''.

                " Evaluation of abnormality

                lv_pythia_abnormality = ls_pythia_response-value.

                if ( lv_pythia_abnormality = '1' ).
                  wa_zsmartmonitorlog-anomaly_detected = 'X'.
                else.
                  wa_zsmartmonitorlog-anomaly_detected = ''.
                endif.

                " Setting suppression challenge found  if there is no anomaly detected
                " Still we need to check other triggered metrics if there are anomalies

                " Actions codes:

                " - - Suppression decision made: positive (to suppress)
                " + - Suppression decision made: negative (to keep alert)
                " ? - MLE reply is empty

                " A - Alert triggering
                " L - long anomaly

                if ( wa_zsmartmonitorlog-anomaly_detected <> 'X').

                  lv_suppression_challenge = 'X'.
                  "      wa_zsmartmonitorlog-challenge = 'X'.
                  "      wa_zsmartmonitorlog-action = '?'.

                  if ( lv_supp_chall_met_list is initial ).

                    lv_supp_chall_met_list = lv_metric_line.
                    " wa_zsmartmonitorlog-metric_tname = lv_metric_tech_name.

                  else.
                    concatenate lv_supp_chall_met_list '|' lv_metric_line into lv_supp_chall_met_list.
                    "     CONCATENATE wa_zsmartmonitorlog-metric_tname '|' lv_metric_tech_name INTO wa_zsmartmonitorlog-metric_tname.
                  endif.

                  concatenate  'Suppression reason candidate found on metric:' lv_metric_id into lv_log_record_text separated by space.
                  log_smartmon_record(
                     ip_msgty  = 'I'
                     ip_log_record_text = lv_log_record_text ).

                else.

                  " If there is either AT LEAST ONE anomaly detected it is enough to trust the alert
                  " Stopping processing immediately and exiting from a loop
                  " Dropping suppression challenge flag

                  concatenate 'Found at least one metric with smart threshold OR anomaly:' 'closing sequence' into lv_log_record_text separated by space.
                  log_smartmon_record(
                     ip_msgty  = 'I'
                     ip_log_record_text = lv_log_record_text ).

                  lv_suppression_challenge = ''.

                  wa_zsmartmonitorlog-action = '+'.
                  wa_zsmartmonitorlog-metric_details = lv_metric_line.

                  if ( lv_noreply_detected = 'X' ).
                    wa_zsmartmonitorlog-no_reply_from_mle = 'X'.
                  endif.

                  get run time field lv_exec_time_end.
                  wa_zsmartmonitorlog-processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

                  insert zsmartmonitorlog from wa_zsmartmonitorlog.

                  exit.

                endif. " IF ( wa_zsmartmonitorlog-anomaly_detected <> 'X')

              else. " IF ( ls_pythia_response-code = 0 ).


                lv_noreply_challenge = 'X'.
                lv_noreply_detected = 'X'.

                concatenate lv_metric_line '/NOT KNOWN TO MLE' into lv_metric_line.

                if ( lv_supp_chall_met_list is initial ).

                  lv_supp_chall_met_list = lv_metric_line.
                  " wa_zsmartmonitorlog-metric_tname = lv_metric_tech_name.

                else.
                  concatenate lv_supp_chall_met_list '|' lv_metric_line into lv_supp_chall_met_list.
                  "     CONCATENATE wa_zsmartmonitorlog-metric_tname '|' lv_metric_tech_name INTO wa_zsmartmonitorlog-metric_tname.
                endif.

                concatenate 'Pythia has not returned 0 code:' 'further processing ' into lv_log_record_text separated by space.

                log_smartmon_record(
                   ip_msgty  = 'I'
                   ip_log_record_text = lv_log_record_text ).

              endif. " IF ( ls_pythia_response-code = 0 ).

              "   INSERT zsmartmonitorlog FROM wa_zsmartmonitorlog.

            else.

              concatenate 'Identified preceeding A events in log:' 'skipping run' into lv_log_record_text separated by space.

              log_smartmon_record(
                 ip_msgty  = 'I'
                 ip_log_record_text = lv_log_record_text ).



            endif. "   IF ( lv_context_id IS NOT INITIAL ) AND ( lv_metric_id IS NOT INITIAL ).

          endif. "IF ( <gt_events>-obj_type = 'M' ) AND ( <gt_events>-rating <> 'Green')

        endloop. "LOOP AT gt_events ASSIGNING <gt_events>.

        " If there are no anomalies found finally confirming alerrt

        if ( lv_suppression_challenge = 'X' ).

          " Automatic confirmation procedure

          " Getting latest ID from ZSMARTMONITORLOG

          select record_id into lv_record_id from zsmartmonitorlog up to 1 rows
          order by record_id descending.

            lv_record_id = lv_record_id + 1.

          endselect.

          " Creating a separate record that alert was suppressed

          wa_zsmartmonitorlog-record_id = lv_record_id.
          wa_zsmartmonitorlog-alert_guid = lv_alert_guid.
          wa_zsmartmonitorlog-context_id = lv_context_id.
          wa_zsmartmonitorlog-metric_details = lv_supp_chall_met_list.
          wa_zsmartmonitorlog-metric_tname = lv_metric_tech_name. " for suppression we log just last metric
          wa_zsmartmonitorlog-metric_id = lv_metric_id.

          wa_zsmartmonitorlog-rating = cl_alert_consm_utility=>get_domain_value_text(
            i_domain_name = cl_alert_consm_constants=>ac_domname_rating
            i_value = <lfs_alert>->get_rating( ) ).

          wa_zsmartmonitorlog-start_timestamp = lv_alert_timestamp_app.
          wa_zsmartmonitorlog-event_date = lv_date_app.
          wa_zsmartmonitorlog-anomaly_detected = ''.
          wa_zsmartmonitorlog-challenge = ''.
          wa_zsmartmonitorlog-suppressed = 'X'.

          wa_zsmartmonitorlog-action = '-'.

          if ( lv_noreply_detected = 'X' ).
            wa_zsmartmonitorlog-no_reply_from_mle = 'X'.
          endif.

          get run time field lv_exec_time_end.
          wa_zsmartmonitorlog-processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

          insert zsmartmonitorlog from wa_zsmartmonitorlog.

          insert wa_zsmartmonitorlog-alert_guid into table lt_suppressed_alerts.

          loop at lt_suppressed_alerts assigning <ls_suppressed_alerts>.

            concatenate 'Confirming alert with ID:' <ls_suppressed_alerts> into lv_log_record_text separated by space.

            log_smartmon_record(
               ip_msgty  = 'I'
               ip_log_record_text = lv_log_record_text ).

            concatenate  'Calling confirmator code:' 'async mode' into lv_log_record_text separated by space.
            log_smartmon_record(
               ip_msgty  = 'I'
               ip_log_record_text = lv_log_record_text ).

            " Calling confirmator code in async mode with delay to wait alert creation completion

            select single value from zsmartmonparam into lv_suppression_mode
                  where param = 'FALSE_ALERT_ACTION'.

            condense lv_suppression_mode.

            if  ( lv_suppression_mode = 'S' ).


              call function 'ZSMARTMON_CONFIRMATOR' starting new task 'ZSUPP'
                exporting
                  et_suppressed_alerts   = lt_suppressed_alerts
                  ip_context_id          = lv_context_id
                  ip_supp_chall_met_list = lv_supp_chall_met_list.


            endif.

          endloop. " LOOP AT lt_suppressed_alerts ASSIGNING <ls_suppressed_alerts>

        endif.   "IF ( lv_suppression_challenge = 'X' ).

        " Adding an extra line, if there are no metrics known to MLE

        if ( lv_noreply_challenge = 'X' ).

          wa_zsmartmonitorlog-action = '?'.
          wa_zsmartmonitorlog-no_reply_from_mle = 'X'.

          if ( ls_pythia_response is not initial ).
            wa_zsmartmonitorlog-metric_details = lv_metric_line.
          else.
            wa_zsmartmonitorlog-metric_details = 'MLE: HTTPS communication failure'.
          endif.


          get run time field lv_exec_time_end.
          wa_zsmartmonitorlog-processing_time = ( ( lv_exec_time_end - lv_exec_time_start ) / 1000 ).

          insert zsmartmonitorlog from wa_zsmartmonitorlog.

        endif.

      endif. "  IF lv_object_type = 'A'

    endloop. " LOOP AT ipt_alerts ASSIGNING <lfs_alert>

    lv_log_record_text = 'Closing alerts evaluation sequence'.

    log_smartmon_record(
       ip_msgty  = 'I'
       ip_log_record_text = lv_log_record_text ).

  endmethod.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method ZCL_INTELLIGENT_ALERTING->IF_ALERT_REACTION~REACT_TO_CLOSED_ALERT
* +-------------------------------------------------------------------------------------------------+
* | [--->] IO_ALERT                       TYPE REF TO IF_ALERT_CONSM_OBJECT
* | [--->] IV_XML                         TYPE        AC_XSTRING
* | [--->] IV_FILTER_VAL                  TYPE        AC_REACTION_ID(optional)
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD if_alert_reaction~react_to_closed_alert.

    " We are marking an alerts in zsmartmonitorlog, when alert group is closed

    DATA: lv_alert_guid  TYPE ac_guid,
          lv_object_type TYPE char10.
    DATA lv_time_char TYPE char8.
    DATA lv_time_date TYPE char8.
    DATA lv_timestamp_char TYPE char20.
    DATA lv_timestamp TYPE e2ea_timestamp.
    DATA lv_start_timestamp TYPE e2ea_timestamp.

    DATA lv_duration_sec TYPE integer.


    " Getting a GUID for an Alert

    lv_object_type = io_alert->get_object_type( ).

    IF lv_object_type = 'A'.

      lv_alert_guid = io_alert->get_id( ).

      lv_time_char  = sy-uzeit.
      lv_time_date = sy-datum.

      CONCATENATE lv_time_date lv_time_char INTO lv_timestamp_char.
      lv_timestamp = lv_timestamp_char.

      SELECT SINGLE start_timestamp INTO lv_start_timestamp FROM zsmartmonitorlog WHERE alert_guid = lv_alert_guid.

      calc_duration_btw_timestamps(
         EXPORTING
         ip_timestamp_1  = lv_start_timestamp
         ip_timestamp_2  = lv_timestamp
         IMPORTING
           ep_duration = lv_duration_sec ).

      " closed_by_system is a final state within alert lifecycle

      UPDATE zsmartmonitorlog
        SET update_timestamp = lv_timestamp
        closed_by_system = 'X'
        duration = lv_duration_sec
          WHERE alert_guid = lv_alert_guid.

    ENDIF. " IF lv_object_type = 'A'

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Static Public Method ZCL_INTELLIGENT_ALERTING=>IMPORT_ANOMALIES_BY_TIMESTAMP
* +-------------------------------------------------------------------------------------------------+
* | [--->] IP_TIME                        TYPE        UZEIT
* | [--->] IP_DATE                        TYPE        DATUM
* | [<---] ET_ABNORMAL_PAIR               TYPE        TT_ABNORMAL_PAIR
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD import_anomalies_by_timestamp.


    DATA lv_json_response TYPE string.
    DATA lv_paramline TYPE char1024.

    DATA: lr_json  TYPE REF TO /ui2/cl_json.
    TYPES: BEGIN OF ty_patrol_response_tt,
             code  TYPE char10,
             value TYPE string,
           END OF ty_patrol_response_tt.

    DATA lt_patrol_response TYPE STANDARD TABLE OF ty_patrol_response_tt.
    DATA ls_patrol_response TYPE ty_patrol_response_tt.

    DATA lv_time_utc_char TYPE char20.

    DATA : lv_ts_6            TYPE p LENGTH 6,
           lv_timezone_sec(5) TYPE p.


    DATA lt_patrol_response_record  TYPE TABLE OF char1024.
    FIELD-SYMBOLS <ls_patrol_response_record> LIKE LINE OF lt_patrol_response_record .

    DATA wa_zsmartmonitorlog TYPE zsmartmonitorlog.

    DATA lv_record_id TYPE i.

    DATA lv_update_record_id TYPE i.

    DATA lv_last_run_id TYPE i.
    DATA lv_next_run_id TYPE i.


    DATA lv_start_timestamp TYPE e2ea_timestamp.
    DATA lv_timestamp TYPE e2ea_timestamp.

    DATA lv_duration TYPE integer.
    DATA lv_duration_char TYPE char8.
    DATA lv_duration_sec TYPE integer.


    TYPES: BEGIN OF ty_patrol_line_tt,
             value TYPE char200,
           END OF ty_patrol_line_tt.

    DATA lt_patrol_line TYPE STANDARD TABLE OF ty_patrol_line_tt.


    TYPES: BEGIN OF ty_abnormal_recs_to_delete_tt,
             context_name TYPE ac_context_name,
             mname        TYPE ac_name,
             anomaly_time TYPE char8,
             anomaly_date TYPE datum,
           END OF ty_abnormal_recs_to_delete_tt.

    DATA lt_abnormal_records_to_delete TYPE STANDARD TABLE OF ty_abnormal_recs_to_delete_tt.
    DATA ls_abnormal_record_to_delete TYPE ty_abnormal_recs_to_delete_tt.

    TYPES: BEGIN OF ty_closed_anomalies_tt,
             record_id TYPE integer,
           END OF ty_closed_anomalies_tt.

    DATA lt_closed_anomalies TYPE STANDARD TABLE OF ty_closed_anomalies_tt.
    DATA ls_closed_anomalies TYPE ty_closed_anomalies_tt.

    DATA ls_abnormal_pair LIKE LINE OF et_abnormal_pair.

    DATA: lv_context_name      TYPE ac_context_name,
          lv_metric_short_text TYPE ac_text_100,
          lv_context_id        TYPE ac_guid,
          lv_event_type_id     TYPE ac_guid,
          lv_temp_string       TYPE char100,
          lv_time              TYPE char8,
          lv_date              TYPE datum.

    DATA lv_time_char TYPE char8.
    DATA lv_time_char_origin TYPE char8.
    DATA lv_time_date TYPE char8.
    DATA lv_timestamp_char TYPE char20.

    DATA lv_t_events_count TYPE i.
    DATA lv_a_events_count TYPE i.

    DATA lt_zsmartmonitorlog_last_batch TYPE TABLE OF zsmartmonitorlog.

    DATA lv_exec_time_main_start TYPE i.
    DATA lv_exec_time_main_end TYPE i.
    DATA lv_exec_time_loop_start TYPE i.
    DATA lv_exec_time_loop_end TYPE i.
    DATA lv_processing_time TYPE ace_d_event_value_sum.


    DATA lv_context_type type AC_CONTEXT_TYPE.

    GET RUN TIME FIELD lv_exec_time_main_start.

    " Getting latest run ID from zsmartmonitorlog

    SELECT run_id INTO lv_last_run_id FROM zsmartmonitorlog UP TO 1 ROWS
    ORDER BY run_id DESCENDING.
    ENDSELECT.

    " Getting latest ID from zsmartmonitorlog

    SELECT record_id INTO lv_record_id FROM zsmartmonitorlog UP TO 1 ROWS
    ORDER BY record_id DESCENDING.
    ENDSELECT.

    " Converting time and date into unix format to be sent to Patrol service
    " During the conversion there's no tzone change, all is in UTC

    PERFORM date_time_to_p6 IN PROGRAM rstr0400 USING
                                          ip_date
                                          ip_time
                                          lv_ts_6
                                          lv_timezone_sec.
    lv_time_utc_char = lv_ts_6.


    " Preparing parameters line to call Patrol service

    CONCATENATE 'timestamp=' lv_time_utc_char INTO lv_paramline.

    CONDENSE lv_paramline NO-GAPS.

    " Calling the Patrol service

    interact_with_mle(
      EXPORTING
       ip_destination = 'ZSMARTPATROL'
       ip_paramline = lv_paramline
      IMPORTING
       ev_json_response = lv_json_response ).

    " Parsing the Patrol response

    CREATE OBJECT lr_json.
    TRY .
        lr_json->deserialize_int( EXPORTING json = lv_json_response CHANGING data = ls_patrol_response ).

      CATCH cx_sy_move_cast_error INTO DATA(lo_move_cast_error) .
    ENDTRY.

    IF ( ( ls_patrol_response-code IS NOT INITIAL ) AND ( ls_patrol_response-code = '0' ) ).

      " --------------------------- Getting a list of currently abnormal events ---------------------

      " Splitting response by records at # sign

      SPLIT ls_patrol_response-value AT '#' INTO TABLE lt_patrol_response_record.

      LOOP AT lt_patrol_response_record ASSIGNING <ls_patrol_response_record>.

        " Splitting a record by:
        " CONTEXT_NAME
        " MNAME
        " M_SHORT_TEXT
        " DATA_COLLECTION_TIMESTAMP
        " abnormalityRating

        SPLIT <ls_patrol_response_record> AT ';' INTO TABLE lt_patrol_line.

        CLEAR ls_abnormal_pair.

        LOOP AT lt_patrol_line ASSIGNING FIELD-SYMBOL(<ls_patrol_line>).

          " Preparing context name
          IF ( ls_abnormal_pair-context_name IS INITIAL ).
            ls_abnormal_pair-context_name = <ls_patrol_line>-value.
            CONTINUE.
          ENDIF.

          " Preparing metric tech name
          IF ( ls_abnormal_pair-mname IS INITIAL ).
            ls_abnormal_pair-mname = <ls_patrol_line>-value.
            CONTINUE.
          ENDIF.

          " Preparing metric short text
          IF ( ls_abnormal_pair-metric_short_text IS INITIAL ).
            ls_abnormal_pair-metric_short_text = <ls_patrol_line>-value.
            CONTINUE.
          ENDIF.

          " Preparing time and date

          IF ( ls_abnormal_pair-anomaly_time IS INITIAL ).
            lv_timestamp = <ls_patrol_line>-value.

            " Converting epoch timestamp into time and date in App Server Time Zone

            PERFORM p6_to_date_time_tz IN PROGRAM rstr0400 USING lv_timestamp
                                                       lv_time
                                                       lv_date.

            ls_abnormal_pair-anomaly_time = lv_time.
            ls_abnormal_pair-anomaly_date = lv_date.
            CONTINUE.
          ENDIF.

          " Preparing anomaly rating

          IF ( ls_abnormal_pair-abnormality_rating IS INITIAL ).
            ls_abnormal_pair-abnormality_rating = <ls_patrol_line>-value.
            CONTINUE.
          ENDIF.

        ENDLOOP. " LOOP AT lt_patrol_line ASSIGNING <ls_patrol_line>.

        IF ( ls_abnormal_pair IS NOT INITIAL ).
          APPEND ls_abnormal_pair TO et_abnormal_pair.
        ENDIF.

      ENDLOOP. "LOOP AT lt_patrol_response_record ASSIGNING <ls_patrol_response_record>.


      SORT et_abnormal_pair BY context_name mname.

      DELETE ADJACENT DUPLICATES FROM et_abnormal_pair COMPARING context_name mname.

      " --------------------------- Processing closed anomalies ---------------------

      " Preparing current timestamp

      lv_time_char  = sy-uzeit.
      lv_time_date = sy-datum.

      REPLACE ALL OCCURRENCES OF ':' IN lv_time_char WITH ''.

      CONCATENATE lv_time_date lv_time_char INTO lv_timestamp_char.
      lv_timestamp = lv_timestamp_char.

      REFRESH lt_zsmartmonitorlog_last_batch.
      REFRESH lt_closed_anomalies.

      " Selecting all previously opened anomalies which are still open

      SELECT record_id context_name metric_tname
        FROM zsmartmonitorlog
        INTO CORRESPONDING FIELDS OF TABLE lt_zsmartmonitorlog_last_batch
        WHERE run_id LE lv_last_run_id
        AND source = 'A'
        AND suppressed <> 'X'
        AND update_timestamp < lv_timestamp.

      CLEAR ls_closed_anomalies.
      REFRESH lt_closed_anomalies.


      IF ( lt_zsmartmonitorlog_last_batch IS NOT INITIAL ).
        LOOP AT lt_zsmartmonitorlog_last_batch ASSIGNING FIELD-SYMBOL(<ls_zsmartmonlog_last_batch>).

          " Checking if anomaly record is presented in last anomaly fetch from Patrol service

          READ TABLE et_abnormal_pair
             WITH KEY context_name = <ls_zsmartmonlog_last_batch>-context_name mname = <ls_zsmartmonlog_last_batch>-metric_tname
             TRANSPORTING NO FIELDS.

          " Previously opened anomaly record is not presented in last anomaly fetch from Patrol service

          IF ( sy-subrc <> 0 ).

            " Remembering record ID for every anomaly which was closed

            ls_closed_anomalies-record_id = <ls_zsmartmonlog_last_batch>-record_id.

            APPEND ls_closed_anomalies TO lt_closed_anomalies.

          ENDIF. "  IF ( sy-subrc <> 0 ).

        ENDLOOP. " LOOP AT lt_zsmartmonitorlog_last_batch ASSIGNING FIELD-SYMBOL(<ls_zsmartmonlog_last_batch>).

        GET RUN TIME FIELD lv_exec_time_main_end.

        " Looping through tables of remebered record IDs to mark closed anomalies in  zsmartmonitorlog

        LOOP AT lt_closed_anomalies ASSIGNING FIELD-SYMBOL(<ls_closed_anomalies>).

          GET RUN TIME FIELD lv_exec_time_loop_start.

          " Calculating timestamps difference

          CLEAR lv_start_timestamp.

          SELECT SINGLE start_timestamp INTO lv_start_timestamp FROM zsmartmonitorlog WHERE record_id = <ls_closed_anomalies>-record_id.


          calc_duration_btw_timestamps(
             EXPORTING
             ip_timestamp_1  = lv_start_timestamp
             ip_timestamp_2  = lv_timestamp
             IMPORTING
               ep_duration = lv_duration_sec ).

          GET RUN TIME FIELD lv_exec_time_loop_end.

          lv_processing_time = ( ( ( lv_exec_time_main_end - lv_exec_time_main_start ) + ( lv_exec_time_loop_end - lv_exec_time_loop_start ) ) / 1000 ).



          UPDATE zsmartmonitorlog SET update_timestamp = lv_timestamp suppressed = 'X' duration = lv_duration_sec processing_time = lv_processing_time
            WHERE record_id = <ls_closed_anomalies>-record_id.

          CLEAR lv_exec_time_loop_start.
          CLEAR lv_exec_time_loop_end.



        ENDLOOP. " LOOP AT lt_closed_anomalies ASSIGNING FIELD-SYMBOL(<ls_closed_anomalies>).

      ENDIF.  " IF ( lt_zsmartmonitorlog_last_batch IS NOT INITIAL )

      " -----------------------------------------------------------------------------------------------
      " Scanning patrol output to identify events processed by T-based monitoring, already registered and closed anomalies

      CLEAR wa_zsmartmonitorlog.

      REFRESH lt_abnormal_records_to_delete.
      CLEAR ls_abnormal_record_to_delete.

      " Setting global batch ID

      lv_next_run_id = lv_last_run_id + 1.

      LOOP AT et_abnormal_pair  ASSIGNING FIELD-SYMBOL(<ls_abnormal_pair>).

        GET RUN TIME FIELD lv_exec_time_loop_start.

        " Check if an event for same object and metric was already detected via static thresholds monitoring (T-based)

        lv_time_char = <ls_abnormal_pair>-anomaly_time.
        lv_time_date = <ls_abnormal_pair>-anomaly_date.

        " We need tp store the original value with : to delete from final output table

        lv_time_char_origin = lv_time_char.

        REPLACE ALL OCCURRENCES OF ':' IN lv_time_char WITH ''.

        " Preparing a timestamp

        CONCATENATE lv_time_date lv_time_char INTO lv_timestamp_char.

        lv_timestamp = lv_timestamp_char.

        " Checking if there are T records detected earlier (T-event)

        SELECT COUNT(*) INTO lv_t_events_count FROM zsmartmonitorlog UP TO 1 ROWS
            WHERE context_name = <ls_abnormal_pair>-context_name
            AND metric_tname = <ls_abnormal_pair>-mname
            AND source = 'T'
            AND closed_by_system <> 'X'
            AND run_id < lv_next_run_id.

        IF ( lv_t_events_count > 0 ).

          wa_zsmartmonitorlog-collision = 'X'.

        ENDIF.

        " Check if same anomaly was already opened earlier (A-event)

        SELECT COUNT(*) INTO lv_a_events_count FROM zsmartmonitorlog UP TO 1 ROWS
            WHERE context_name = <ls_abnormal_pair>-context_name
            AND metric_tname = <ls_abnormal_pair>-mname
            AND source = 'A'
            AND suppressed <> 'X'
            AND run_id < lv_next_run_id.

        IF (  lv_a_events_count = 0 ) .

          " Preparing a new record

          lv_record_id = lv_record_id + 1.

          wa_zsmartmonitorlog-run_id = lv_next_run_id.
          wa_zsmartmonitorlog-record_id = lv_record_id.
          wa_zsmartmonitorlog-source = 'A'.
          wa_zsmartmonitorlog-context_id = 'context_id not found by context_name'.
          wa_zsmartmonitorlog-context_type = 'not found'.
          wa_zsmartmonitorlog-metric_id = 'event_type_id not found by mname'.

          " Taking system valid context_id and event_type id

          SELECT SINGLE context_id context_type FROM v_acentrypoints
                 INTO ( lv_context_id, lv_context_type )
                 WHERE context_name = <ls_abnormal_pair>-context_name
                 AND ac_variant = 'A'.

          IF sy-subrc = 0.
            wa_zsmartmonitorlog-context_id = lv_context_id.
            wa_zsmartmonitorlog-context_type = lv_context_type.

            SELECT SINGLE event_type_id FROM acmetricdir INTO lv_event_type_id
              WHERE name = <ls_abnormal_pair>-mname AND
              ac_variant = 'A' AND
              context_id = lv_context_id
              AND event_class EQ cl_alrt_cfg_constants=>ac_entity_metric.

            IF sy-subrc = 0.
              wa_zsmartmonitorlog-metric_id = lv_event_type_id.
            ENDIF. " IF sy-subrc = 0.

          ENDIF. " IF sy-subrc = 0

          wa_zsmartmonitorlog-context_name = <ls_abnormal_pair>-context_name.
          wa_zsmartmonitorlog-metric_tname = <ls_abnormal_pair>-mname.


          wa_zsmartmonitorlog-abnormality_rating = <ls_abnormal_pair>-abnormality_rating.
          wa_zsmartmonitorlog-event_date = lv_time_date.
          wa_zsmartmonitorlog-event_time = lv_time_char_origin.


          wa_zsmartmonitorlog-start_timestamp = lv_timestamp_char.
          wa_zsmartmonitorlog-update_timestamp = lv_timestamp_char.

          " New anomaly -> duration is zero

          wa_zsmartmonitorlog-duration = 0.
          wa_zsmartmonitorlog-action = '!'.
          wa_zsmartmonitorlog-tech_scenario = 'ANOMALYDET'.
          wa_zsmartmonitorlog-rating = 'Anomaly'.
          wa_zsmartmonitorlog-metric_details = <ls_abnormal_pair>-metric_short_text.

          GET RUN TIME FIELD lv_exec_time_loop_end.

          lv_processing_time = ( ( ( lv_exec_time_main_end - lv_exec_time_main_start ) + ( lv_exec_time_loop_end - lv_exec_time_loop_start ) ) / 1000 ).
          wa_zsmartmonitorlog-processing_time = lv_processing_time.

          INSERT zsmartmonitorlog FROM wa_zsmartmonitorlog.

          CLEAR lv_exec_time_loop_start.
          CLEAR lv_exec_time_loop_end.

        ELSE. " Detected registered events in zsmartmonitorlog of A type

          " Recording all records in a separate table to remove for collector output

          ls_abnormal_record_to_delete-context_name = <ls_abnormal_pair>-context_name.
          ls_abnormal_record_to_delete-mname = <ls_abnormal_pair>-mname.
          ls_abnormal_record_to_delete-anomaly_time =  lv_time_char_origin.
          ls_abnormal_record_to_delete-anomaly_date =  lv_time_date.

          APPEND ls_abnormal_record_to_delete TO lt_abnormal_records_to_delete.

          " Changing last collection timestamp for A type events

          lv_time_char  = sy-uzeit.
          lv_time_date = sy-datum.

          REPLACE ALL OCCURRENCES OF ':' IN lv_time_char WITH ''.

          CONCATENATE lv_time_date lv_time_char INTO lv_timestamp_char.

          lv_timestamp = lv_timestamp_char.

          SELECT MAX( record_id )  INTO lv_update_record_id FROM zsmartmonitorlog
            WHERE context_name = <ls_abnormal_pair>-context_name
            AND metric_tname = <ls_abnormal_pair>-mname
            AND source = 'A' AND suppressed <> 'X'
            AND run_id < lv_next_run_id.

          IF  ( lv_update_record_id IS NOT INITIAL ).

            CLEAR lv_start_timestamp.
            CLEAR lv_duration_sec.

            SELECT SINGLE start_timestamp INTO lv_start_timestamp FROM zsmartmonitorlog WHERE record_id = lv_update_record_id.

            calc_duration_btw_timestamps(
               EXPORTING
               ip_timestamp_1  = lv_start_timestamp
               ip_timestamp_2  = lv_timestamp
               IMPORTING
                 ep_duration = lv_duration_sec ).

            GET RUN TIME FIELD lv_exec_time_loop_end.

            lv_processing_time = ( ( ( lv_exec_time_main_end - lv_exec_time_main_start ) + ( lv_exec_time_loop_end - lv_exec_time_loop_start ) ) / 1000 ).

            UPDATE zsmartmonitorlog SET update_timestamp = lv_timestamp action = '~' duration = lv_duration_sec processing_time = lv_processing_time
              WHERE record_id = lv_update_record_id.

            CLEAR lv_exec_time_loop_start.
            CLEAR lv_exec_time_loop_end.

          ENDIF. "   IF  ( lv_update_record_id IS NOT initial ).

        ENDIF. "IF (  lv_a_events_count = 0 )

      ENDLOOP. " LOOP AT et_abnormal_pair  ASSIGNING FIELD-SYMBOL(<ls_abnormal_pair>)

      " Cleaning redundand records in output

      LOOP AT lt_abnormal_records_to_delete ASSIGNING FIELD-SYMBOL(<ls_abnormal_record_to_delete>).

        DELETE et_abnormal_pair
          WHERE context_name = <ls_abnormal_record_to_delete>-context_name
          AND mname = <ls_abnormal_record_to_delete>-mname
          AND anomaly_date = <ls_abnormal_record_to_delete>-anomaly_date
          AND anomaly_time = <ls_abnormal_record_to_delete>-anomaly_time.

      ENDLOOP. " LOOP AT lt_abnormal_records_to_delete ASSIGNING FIELD-SYMBOL(<ls_abnormal_record_to_delete>)

    ELSE.

      " Communication failure situation

      GET RUN TIME FIELD lv_exec_time_loop_start.

      lv_next_run_id = lv_last_run_id + 1.
      lv_record_id = lv_record_id + 1.
      wa_zsmartmonitorlog-metric_details = 'MLE: HTTPS communication failure'.

      wa_zsmartmonitorlog-run_id = lv_next_run_id.
      wa_zsmartmonitorlog-record_id = lv_record_id.
      wa_zsmartmonitorlog-no_reply_from_mle = 'X'.


      GET RUN TIME FIELD lv_exec_time_loop_end.
      lv_processing_time = ( ( ( lv_exec_time_main_end - lv_exec_time_main_start ) + ( lv_exec_time_loop_end - lv_exec_time_loop_start ) ) / 1000 ).


      wa_zsmartmonitorlog-processing_time = lv_processing_time.

      INSERT zsmartmonitorlog FROM wa_zsmartmonitorlog.

      CLEAR lv_exec_time_loop_start.
      CLEAR lv_exec_time_loop_end.

    ENDIF. " IF ( ( ls_patrol_response-code IS NOT INITIAL ) and ( ls_patrol_response-code = '0' ) )

  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Static Private Method ZCL_INTELLIGENT_ALERTING=>INTERACT_WITH_MLE
* +-------------------------------------------------------------------------------------------------+
* | [--->] IP_DESTINATION                 TYPE        RFCDEST
* | [--->] IP_PARAMLINE                   TYPE        CHAR1024
* | [<---] EV_JSON_RESPONSE               TYPE        STRING
* +--------------------------------------------------------------------------------------</SIGNATURE>
  METHOD interact_with_mle.


    DATA: lo_http_client TYPE REF TO if_http_client,
          lo_rest_client TYPE REF TO cl_rest_http_client,
          lv_url         TYPE        string,
          lv_body        TYPE        string,
          token          TYPE        string,
          agreements     TYPE        string,
          lo_response    TYPE REF TO     if_rest_entity.

    DATA lv_log_record_text TYPE string.

    cl_http_client=>create_by_destination(
     EXPORTING
       destination              = ip_destination    " Logical destination (specified in function call)
     IMPORTING
       client                   = lo_http_client    " HTTP Client Abstraction
     EXCEPTIONS
       argument_not_found       = 1
       destination_not_found    = 2
       destination_no_authority = 3
       plugin_not_active        = 4
       internal_error           = 5
       OTHERS                   = 6
    ).

    IF ( sy-subrc <> 0 ).

      CASE sy-subrc.
        WHEN '1'.
          lv_log_record_text = 'argument_not_found'.
        WHEN '2'.
          lv_log_record_text = 'destination_not_found'.
        WHEN '3'.
          lv_log_record_text = 'destination_no_authority'.
        WHEN '4'.
          lv_log_record_text = 'plugin_not_active'.
        WHEN '5'.
          lv_log_record_text = 'internal_error'.
        WHEN OTHERS.
          lv_log_record_text ='not_known_exception'.

      ENDCASE.

      CONCATENATE 'HTTPS destination error:' lv_log_record_text  INTO lv_log_record_text SEPARATED BY space.

      log_smartmon_record(
             ip_msgty  = 'E'
             ip_log_record_text = lv_log_record_text ).

      ev_json_response = ''.

      EXIT.

    ENDIF. " IF ( sy_subrc <> 0 )


* Create REST client instance

    CREATE OBJECT lo_rest_client
      EXPORTING
        io_http_client = lo_http_client.

* Set HTTP version

    lo_http_client->request->set_version( if_http_request=>co_protocol_version_1_0 ).

    IF lo_http_client IS BOUND AND lo_rest_client IS BOUND.

      " Adding parameters to call

      CONCATENATE '?' ip_paramline INTO lv_url.

      " Set the URI

      cl_http_utility=>set_request_uri(
          EXPORTING
            request = lo_http_client->request    " HTTP Framework (iHTTP) HTTP Request
            uri     = lv_url                     " URI String (in the Form of /path?query-string)
        ).

      " Set request header if any
      CALL METHOD lo_rest_client->if_rest_client~set_request_header
        EXPORTING
          iv_name  = 'auth-token'
          iv_value = token.

      " HTTP GET

      TRY.

          lo_rest_client->if_rest_client~get( ).

        CATCH cx_rest_client_exception INTO DATA(lo_rest_client_error) .

          lv_log_record_text = lo_rest_client_error->get_text( ).

          log_smartmon_record(
              ip_msgty  = 'E'
              ip_log_record_text = lv_log_record_text ).

          ev_json_response = ''.

          EXIT.

      ENDTRY.

      " HTTP response

      lo_response = lo_rest_client->if_rest_client~get_response_entity( ).

      " HTTP return status

      DATA(http_status)   = lo_response->get_header_field( '~status_code' ).

    ENDIF. " IF lo_http_client IS BOUND AND lo_rest_client IS BOUND

    " Filling response

    ev_json_response = lo_response->get_string_data( ).
  ENDMETHOD.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Static Public Method ZCL_INTELLIGENT_ALERTING=>LOG_SMARTMON_RECORD
* +-------------------------------------------------------------------------------------------------+
* | [--->] IP_MSGTY                       TYPE        SYMSGTY
* | [--->] IP_LOG_RECORD_TEXT             TYPE        STRING
* +--------------------------------------------------------------------------------------</SIGNATURE>
  method LOG_SMARTMON_RECORD.

    " Data declaration for Application Log operations

  DATA ls_log      TYPE bal_s_log.
  DATA lv_msgtext  TYPE baltmsg.
  DATA ev_log_handle  TYPE balloghndl.

  DATA ls_msg TYPE bal_s_msg.

  DATA:
    BEGIN OF ls_string,
      part1 TYPE symsgv,
      part2 TYPE symsgv,
      part3 TYPE symsgv,
      part4 TYPE symsgv,
    END OF ls_string.

  DATA: lt_log_handle TYPE bal_t_logh,
        lt_log_num    TYPE bal_t_lgnm.

  DATA ev_subrc  TYPE sysubrc.


 CONCATENATE sy-uzeit ip_log_record_text INTO ls_string SEPARATED BY space.

  " Preparing the Application Log

  CLEAR ev_log_handle.

  ls_log-object    = 'ZSMARTMON'.
  ls_log-subobject = 'ZSUPPRESSION'.
  ls_log-aldate    = sy-datum.
  ls_log-altime    = sy-uzeit.
  ls_log-aluser    = sy-uname.

  CALL FUNCTION 'BAL_LOG_CREATE'
    EXPORTING
      i_s_log                 = ls_log
    IMPORTING
      e_log_handle            = ev_log_handle
    EXCEPTIONS
      log_header_inconsistent = 1
      OTHERS                  = 2.
  IF sy-subrc <> 0.
    ev_subrc = 1.
    RETURN.
  ENDIF.

  ls_msg-msgv1     = ls_string-part1.
  ls_msg-msgv2     = ls_string-part2.
  ls_msg-msgv3     = ls_string-part3.
  ls_msg-msgv4     = ls_string-part4.

  ls_msg-msgty = IP_MSGTY.
  ls_msg-msgid = 'BL'.
  ls_msg-msgno = '001'.

  CALL FUNCTION 'BAL_LOG_MSG_ADD'
    EXPORTING
      i_log_handle  = ev_log_handle
      i_s_msg       = ls_msg
    EXCEPTIONS
      log_not_found = 0
      OTHERS        = 1.

  IF sy-subrc <> 0.
    MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno
    WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.

  ENDIF.

  " Finalizing the Application Log records

  INSERT ev_log_handle INTO lt_log_handle INDEX 1.

  CALL FUNCTION 'BAL_DB_SAVE'
    EXPORTING
      i_client         = sy-mandt
      i_save_all       = ' '
      i_t_log_handle   = lt_log_handle
    IMPORTING
      e_new_lognumbers = lt_log_num
    EXCEPTIONS
      log_not_found    = 1
      save_not_allowed = 2
      numbering_error  = 3
      OTHERS           = 4.
  IF sy-subrc <> 0.
    ev_subrc = sy-subrc.
  ENDIF.



  endmethod.
ENDCLASS.