import pandas as pd
import numpy as np

def filter_production_data(df):
    """
    ฟังก์ชันสำหรับกรองข้อมูลการผลิตตามเงื่อนไขที่กำหนด
    """
    # ตรวจสอบว่ามีคอลัมน์ที่จำเป็นครบหรือไม่
    required_columns = ['ANET', 'QTY', 'Style', 'Asst', 'Zone']
    for col in required_columns:
        if col not in df.columns:
            print(f"❌ Production file ไม่มีคอลัมน์ {col}")
            return None

    # แปลงคอลัมน์ที่เกี่ยวข้องให้เป็นตัวเลข
    df['ANET'] = pd.to_numeric(df['ANET'], errors='coerce')
    df['QTY'] = pd.to_numeric(df['QTY'], errors='coerce')

    # ลบแถวที่ QTY ว่างหรือเป็น 0
    df_filtered = df.dropna(subset=['QTY'])
    df_filtered = df_filtered[df_filtered['QTY'] != 0]

    # เงื่อนไขการกรองตามโจทย์: 'A Net' เป็น 0 หรือ 'A Net' >= 'QTY' / 3
    condition = (df_filtered['ANET'] == 0) | (df_filtered['ANET'] >= df_filtered['QTY'] / 3)
    df_filtered = df_filtered[condition].copy()

    # เพิ่มคอลัมน์ 'linkk' โดยการรวมคอลัมน์ 'Zone' และ 'Style'
    df_filtered['linkk'] = df_filtered['Zone'].astype(str) + df_filtered['Style'].astype(str)
    return df_filtered


def calculate_capacity(df_production, df_capacity):
    """
    ฟังก์ชันสำหรับคำนวณและจัดสรรการผลิตตาม capacity
    """
    # Merge dataframes on the 'linkk' column
    df_merged = pd.merge(df_production, df_capacity, on='linkk', how='left')

    # Drop the duplicate 'Style_y' and 'Zone_y' columns and rename the remaining ones
    df_merged = df_merged.drop(columns=['Style_y', 'Zone_y'])
    df_merged = df_merged.rename(columns={'Style_x': 'Style', 'Zone_x': 'Zone'})

    # Check for missing Capacity values after merge
    if df_merged['Capacity'].isnull().any():
        print("Warning: There are missing 'Capacity' values after merging. This might cause issues with the calculation.")

    # Initialize an empty DataFrame to store the results
    results_df = pd.DataFrame(columns=['Zone', 'Asst', 'Style', 'Cap_per_shift', 'Day', 'Shift', 'Allocated_QTY', 'linkk'])

    # Ensure 'Capacity' column is numeric
    df_merged['Capacity'] = pd.to_numeric(df_merged['Capacity'], errors='coerce')

    # Loop through each unique Zone and apply the allocation logic
    for zone, group in df_merged.groupby('Zone'):
        current_day = 1
        remaining_A_capacity = 0
        remaining_B_capacity = 0

        # Sort the group by 'Issue date' for consistent allocation order
        group = group.sort_values(by='Issue date')

        for _, row in group.iterrows():
            asst = row['Asst']
            style = row['Style']
            QTY_to_allocate = row['QTY']
            cap_per_shift = row['Capacity']
            linkk = row['linkk']

            if pd.isna(cap_per_shift):
                # Skip rows where Capacity is not found
                continue

            if remaining_A_capacity == 0 and remaining_B_capacity == 0:
                remaining_A_capacity = cap_per_shift
                remaining_B_capacity = cap_per_shift

            while QTY_to_allocate > 0:
                if remaining_A_capacity > 0:
                    allocated_QTY = min(QTY_to_allocate, remaining_A_capacity)
                    new_row = pd.DataFrame([{
                        'Zone': zone,
                        'Asst': asst,
                        'Style': style,
                        'Cap_per_shift': cap_per_shift,
                        'Day': current_day,
                        'Shift': 'A',
                        'Allocated_QTY': allocated_QTY,
                        'linkk': linkk
                    }])
                    results_df = pd.concat([results_df, new_row], ignore_index=True)

                    QTY_to_allocate -= allocated_QTY
                    remaining_A_capacity -= allocated_QTY

                if QTY_to_allocate > 0 and remaining_B_capacity > 0:
                    allocated_QTY = min(QTY_to_allocate, remaining_B_capacity)
                    new_row = pd.DataFrame([{
                        'Zone': zone,
                        'Asst': asst,
                        'Style': style,
                        'Cap_per_shift': cap_per_shift,
                        'Day': current_day,
                        'Shift': 'B',
                        'Allocated_QTY': allocated_QTY,
                        'linkk': linkk
                    }])
                    results_df = pd.concat([results_df, new_row], ignore_index=True)

                    QTY_to_allocate -= allocated_QTY
                    remaining_B_capacity -= allocated_QTY

                if QTY_to_allocate > 0:
                    current_day += 1
                    remaining_A_capacity = cap_per_shift
                    remaining_B_capacity = cap_per_shift
    
    return results_df

# ---- Main Execution Block ----
if __name__ == "__main__":
    production_filename = 'production_data_master.csv'  # หรือชื่อไฟล์ master ของคุณ
    capacity_filename = 'capacity_data.csv'

    try:
        # Read the raw production data
        df_prod_master = pd.read_csv(production_filename)
        print("📄 Production Data (Raw):")
        print(df_prod_master.head(10))

        # Filter the production data
        df_filtered = filter_production_data(df_prod_master)
        
        if df_filtered is not None:
            # Read the capacity data
            df_capacity = pd.read_csv(capacity_filename)
            
            # Perform the capacity calculation using the filtered data
            results_df = calculate_capacity(df_filtered, df_capacity)

            # Write the final DataFrame to a CSV file
            output_filename = 'calculated_production_capacity11.csv'
            results_df.to_csv(output_filename, index=False)

            print(f"\n✅ Production data is filtered and calculation is complete.")
            print(f"📥 บันทึกผลลัพธ์เรียบร้อย: {output_filename}")
            print("\n--- First 5 rows of the result ---")
            print(results_df.head())

    except FileNotFoundError as e:
        print(f"❌ Error: {e}. กรุณาวางไฟล์ที่จำเป็น (production_data_master.csv และ capacity_data.csv) ในโฟลเดอร์เดียวกันแล้วลองใหม่")
