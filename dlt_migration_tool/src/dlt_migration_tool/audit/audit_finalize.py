# Add audit tracking to the end of the notebook
def finalize_audit():
    """
    Finalize the audit tracking and provide a summary.
    """
    try:
        # End the audit run
        audit.end_run(status="SUCCESS")
        
        # Get and print summary
        summary = audit.get_run_summary()
        
        print("\n" + "=" * 80)
        print(f"AUDIT SUMMARY - RUN ID: {summary['run_id']}")
        print("=" * 80)
        print(f"Source System: {summary['source_system']}")
        print(f"Environment: {summary['environment']}")
        print(f"User: {summary['user']}")
        print(f"Start Time: {summary['start_time']}")
        print(f"End Time: {summary['end_time']}")
        
        if summary['duration_seconds']:
            minutes, seconds = divmod(summary['duration_seconds'], 60)
            print(f"Duration: {int(minutes)} minutes, {int(seconds)} seconds")
        
        print(f"Status: {summary['status']}")
        print(f"Tables Processed: {summary['tables_processed']}")
        print(f"Failures: {summary['failures']}")
        print(f"Artifacts Generated: {summary['artifacts_generated']}")
        
        # Print tables summary
        tables_summary = audit.get_tables_summary()
        if tables_summary:
            print("\nTABLES PROCESSED:")
            for table in tables_summary:
                status = "✅" if table.get("status") == "SUCCESS" else "❌"
                print(f"{status} {table['table_name']}")
                print(f"   Bronze: {table.get('bronze_target', 'N/A')}")
                print(f"   Silver: {table.get('silver_target', 'N/A')}")
        
        # Print failures
        failures = audit.get_failures()
        if failures:
            print("\nFAILURES:")
            for failure in failures:
                print(f"❌ {failure['table_name']}: {failure['error_message']}")
        
        print("=" * 80)
        print(f"Audit data stored in: {UC_CATALOG}.{UC_SCHEMA}.dlt_migration_runs")
        print(f"Run details available with: SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.dlt_migration_runs WHERE run_id = '{summary['run_id']}'")
        print("=" * 80)
        
        return summary
    except Exception as e:
        print(f"Error finalizing audit: {str(e)}")
        print("Stack trace:")
        import traceback
        traceback.print_exc()
        
        # Try to end the run with failure status
        try:
            audit.end_run(status="FAILED", error_message=str(e))
        except:
            pass
        
        return None

# Call finalize_audit at the end of the notebook
finalize_audit()
