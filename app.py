import streamlit as st
import pandas as pd
import numpy as np
from io import StringIO

# ------------------------------
# ฟังก์ชันสำหรับกรองข้อมูล
# ------------------------------
def filter_production_data(df):
    required_columns = ['ANET', 'QTY', 'Style', 'Asst', 'Zone']
    for col in required_columns:
        if col not in df.columns:
            st.error(f"❌ Production file ไม่มีคอลัมน์ {col}")
            return None

    df['ANET'] = pd.to_numeric(df['ANET'], errors='coerce')
    df['QTY'] = pd.to_numeric(df['QTY'], errors='coerce')

    df_filtered = df.dropna(subset=['QTY'])
    df_filtered = df_filtered[df_filtered['QTY'] != 0].copy()

    condition = (df_filtered['ANET'] == 0) | (df_filtered['ANET'] >= df_filtered['QTY'] / 3)
    df_filtered = df_filtered[condition]

    df_filtered['linkk'] = df_filtered['Zone'].astype(str) + df_filtered['Style'].astype(str)
    return df_filtered


# ------------------------------
# ฟังก์ชันคำนวณ capacity allocation
# ------------------------------
def calculate_capacity(df_production, df_capacity):
    df_merged = pd.merge(df_production, df_capacity, on='linkk', how='left')

    for col in ['Style_y', 'Zone_y']:
        if col in df_merged.columns:
            df_merged = df_merged.drop(columns=[col])

    df_merged = df_merged.rename(columns={'Style_x': 'Style', 'Zone_x': 'Zone'})

    df_merged['Capacity'] = pd.to_numeric(df_merged['Capacity'], errors='coerce')

    results_df = pd.DataFrame(columns=[
        'Zone', 'Asst', 'Style', 'Cap_per_shift', 'Day', 'Shift',
        'Allocated_QTY', 'linkk', 'Group', 'Color', 'Size', 'Original_QTY'
    ])

    for zone, group in df_merged.groupby('Zone'):
        current_day = 1
        remaining_A_capacity = 0
        remaining_B_capacity = 0
        group = group.sort_values(by='Issue date')

        for _, row in group.iterrows():
            asst = row['Asst']
            style = row['Style']
            QTY_to_allocate = row['QTY']
            original_qty = row['QTY']
            cap_per_shift = row['Capacity']
            linkk = row['linkk']
            group_val = row.get('Group', None)
            color_val = row.get('Color', None)
            size_val = row.get('Size', None)

            if pd.isna(cap_per_shift):
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
                        'linkk': linkk,
                        'Group': group_val,
                        'Color': color_val,
                        'Size': size_val,
                        'Original_QTY': original_qty
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
                        'linkk': linkk,
                        'Group': group_val,
                        'Color': color_val,
                        'Size': size_val,
                        'Original_QTY': original_qty
                    }])
                    results_df = pd.concat([results_df, new_row], ignore_index=True)
                    QTY_to_allocate -= allocated_QTY
                    remaining_B_capacity -= allocated_QTY

                if QTY_to_allocate > 0:
                    current_day += 1
                    remaining_A_capacity = cap_per_shift
                    remaining_B_capacity = cap_per_shift

    return results_df


# ------------------------------
# ส่วนของ Streamlit UI
# ------------------------------
st.set_page_config(page_title="Production Capacity Calculator", page_icon="📊", layout="wide")

st.title("📦 Production Capacity Calculator")
st.markdown("อัปโหลดไฟล์ข้อมูลการผลิต และ capacity เพื่อคำนวณจัดสรรกำลังผลิตอัตโนมัติ")

# อัปโหลดไฟล์ CSV
prod_file = st.file_uploader("📁 Upload Production File (CSV)", type="csv")
cap_file = st.file_uploader("📁 Upload Capacity File (CSV)", type="csv")

if prod_file and cap_file:
    df_prod = pd.read_csv(prod_file)
    df_cap = pd.read_csv(cap_file)

    st.subheader("📋 ข้อมูลการผลิต (ตัวอย่าง)")
    st.dataframe(df_prod.head(10))

    st.subheader("⚙️ ข้อมูล Capacity (ตัวอย่าง)")
    st.dataframe(df_cap.head(10))

    if st.button("▶️ Run Calculation"):
        with st.spinner("กำลังคำนวณ..."):
            df_filtered = filter_production_data(df_prod)
            if df_filtered is not None:
                result = calculate_capacity(df_filtered, df_cap)

                st.success("✅ คำนวณเสร็จเรียบร้อย!")
                st.subheader("📊 ผลลัพธ์ตัวอย่าง")
                st.dataframe(result.head(20))

                # ปุ่มดาวน์โหลด
                csv_output = result.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="💾 Download Result CSV",
                    data=csv_output,
                    file_name="calculated_production_capacity.csv",
                    mime="text/csv"
                )

else:
    st.info("👆 กรุณาอัปโหลดไฟล์ทั้งสองก่อนเริ่มการคำนวณ")
