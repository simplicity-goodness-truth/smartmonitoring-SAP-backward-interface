FUNCTION ZSMARTMON_CONFIRMATOR.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     REFERENCE(ET_SUPPRESSED_ALERTS) TYPE  E2EA_T_GUID
*"     REFERENCE(IP_CONTEXT_ID) TYPE  AC_GUID
*"     REFERENCE(IP_SUPP_CHALL_MET_LIST) TYPE  STRING
*"----------------------------------------------------------------------


  DATA lv_log_record_text TYPE string.

  " Structures for alerts confirmation

  DATA lo_acc TYPE REF TO cl_alert_consumer_connector.

  DATA lv_count TYPE i.
  DATA lv_count_char TYPE char10.

  DATA lr_db_exception TYPE REF TO cx_alert_consm_database.
  DATA lr_consm_exception TYPE REF TO cx_alert_consm_connector.

  DATA lv_comment_text TYPE e2ea_comments.

  lv_log_record_text = 'Entering a code of confirmator'.

  zcl_intelligent_alerting=>log_smartmon_record(
        ip_msgty  = 'I'
        ip_log_record_text = lv_log_record_text ).

  WAIT UP TO 3 SECONDS.

  lv_log_record_text = 'Executing 3 seconds delayed code of confirmator'.

  zcl_intelligent_alerting=>log_smartmon_record(
        ip_msgty  = 'I'
        ip_log_record_text = lv_log_record_text ).


  CONCATENATE 'Metrics considered:' ip_supp_chall_met_list INTO lv_comment_text SEPARATED BY space.

  TRY.

      CREATE OBJECT lo_acc.

      lo_acc->if_alert_consumer_connector~confirm_alert_groups(
        EXPORTING
          it_algroupid = et_suppressed_alerts
          i_comments = lv_comment_text
          i_category_id = 'Z01'
          i_classification_id = 'Z01'
        IMPORTING
          e_incident_alert  = lv_count ).

      lv_count_char = lv_count.


      CONCATENATE  'Confirmation finished with count of opened incidents' lv_count_char INTO lv_log_record_text SEPARATED BY space.


      zcl_intelligent_alerting=>log_smartmon_record(
            ip_msgty  = 'I'
            ip_log_record_text = lv_log_record_text ).

    CATCH cx_alert_consm_database INTO lr_db_exception.
    CATCH cx_alert_consm_connector INTO lr_consm_exception.

  ENDTRY.

  lv_log_record_text = 'Leaving code of confirmator' .

  zcl_intelligent_alerting=>log_smartmon_record(
        ip_msgty  = 'I'
        ip_log_record_text = lv_log_record_text ).



ENDFUNCTION.