#!/bin/bash

# Script to run the DLT Migration Tool and Schema Generator
# Author: Goose AI
# Date: August 13, 2025

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Set the base directory
BASE_DIR="/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool"
cd "$BASE_DIR" || { echo -e "${RED}Failed to change to base directory${NC}"; exit 1; }

# Function to display section headers
section() {
    echo -e "\n${BLUE}===========================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===========================================================${NC}\n"
}

# Function to activate virtual environment
activate_venv() {
    section "Activating Virtual Environment"
    if [ -d "venv" ]; then
        source venv/bin/activate
        echo -e "${GREEN}Virtual environment activated${NC}"
    else
        echo -e "${YELLOW}Warning: Virtual environment not found. Creating one...${NC}"
        python -m venv venv
        source venv/bin/activate
        pip install -e .
        echo -e "${GREEN}Virtual environment created and activated${NC}"
    fi
}

# Function to run the migration tool
run_migration_tool() {
    section "Running DLT Migration Tool"
    echo -e "Processing input file: ${YELLOW}samples/input/a_locations_maximo_onboarding.json${NC}"
    
    python src/dlt_migration_tool/cli.py \
        -i samples/input/a_locations_maximo_onboarding.json \
        -o samples/output \
        -v
    
    if [ $? -eq 0 ]; then
        echo -e "\n${GREEN}Migration completed successfully${NC}"
    else
        echo -e "\n${RED}Migration failed${NC}"
        exit 1
    fi
}

# Function to list output files
list_output_files() {
    section "Listing Output Files"
    echo -e "${YELLOW}Bronze layer output files:${NC}"
    ls -la samples/output/bronze_maximo/dataflowspec/
    
    echo -e "\n${YELLOW}Silver layer output files:${NC}"
    ls -la samples/output/silver_maximo/dataflowspec/
}

# Function to generate schema files
generate_schema_files() {
    section "Generating Empty Schema Files"
    python src/dlt_migration_tool/schema_manager.py generate \
        -t edp_bronze_dev.maximo.asset \
        -o samples/output/schemas
    
    if [ $? -eq 0 ]; then
        echo -e "\n${GREEN}Schema files generated successfully${NC}"
    else
        echo -e "\n${RED}Schema file generation failed${NC}"
        exit 1
    fi
}

# Function to update schema files
update_schema_files() {
    section "Updating Schema Files"
    python src/dlt_migration_tool/schema_manager.py update \
        -s samples/output/schemas \
        -c samples/output
    
    if [ $? -eq 0 ]; then
        echo -e "\n${GREEN}Schema files updated successfully${NC}"
    else
        echo -e "\n${RED}Schema file update failed${NC}"
        exit 1
    fi
}

# Function to display instructions for Databricks steps
display_databricks_instructions() {
    section "Databricks Instructions"
    echo -e "${YELLOW}Manual Steps Required:${NC}"
    echo -e "1. Upload the generated Python scripts to Databricks"
    echo -e "2. Run the scripts in Databricks to extract actual schemas"
    echo -e "3. Download the updated schema files from Databricks"
    echo -e "4. Replace the placeholder schemas in samples/output/schemas with the downloaded files"
}

# Main execution
main() {
    # Display welcome message
    section "DLT Migration Tool Workflow"
    echo -e "This script will run the DLT Migration Tool and Schema Generator"
    
    # Ask user what they want to do
    echo -e "\n${YELLOW}Select an option:${NC}"
    echo -e "1) Run migration tool only"
    echo -e "2) Generate and update schema files only"
    echo -e "3) Run full workflow (migration + schema generation)"
    echo -e "4) Exit"
    
    read -p "Enter your choice (1-4): " choice
    
    case $choice in
        1)
            activate_venv
            run_migration_tool
            list_output_files
            ;;
        2)
            activate_venv
            generate_schema_files
            update_schema_files
            display_databricks_instructions
            ;;
        3)
            activate_venv
            run_migration_tool
            list_output_files
            generate_schema_files
            update_schema_files
            display_databricks_instructions
            ;;
        4)
            echo -e "${GREEN}Exiting...${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid choice. Exiting...${NC}"
            exit 1
            ;;
    esac
    
    # Deactivate virtual environment
    deactivate
    echo -e "\n${GREEN}Done! Virtual environment deactivated.${NC}"
}

# Run the main function
main
