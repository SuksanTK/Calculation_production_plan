import streamlit as st
import pandas as pd
import numpy as np

def filter_production_data(df):
    """
    ฟังก์ชันสำหรับกรองข้อมูลการผลิตตามเงื่อนไขที่กำหนด
    """
    required_columns = ['ANET', 'QTY', 'Style', 'Asst', 'Zone', 'Issue date']
    for col in required_columns:
        if col not in df.columns:
            st.error(f"❌ Production file ไม่มีคอลัมน์ที่จำเป็น: {col}")
            return None

    df['ANET'] = pd.to_numeric(df['ANET'], errors='coerce')
    df['QTY'] = pd.to_numeric(df['QTY'], errors='coerce')
    df['Issue date'] = pd.to_numeric(df['Issue date'], errors='coerce')

    df_filtered = df.dropna(subset=['QTY', 'Issue date'])
    df_filtered = df_filtered[df_filtered['QTY'] != 0].copy()

    condition = (df_filtered['ANET'] == 0) | (df_filtered['ANET'] >= df_filtered['QTY'] / 3)
    df_filtered = df_filtered[condition]

    df_filtered['linkk'] = df_filtered['Zone'].astype(str) + df_filtered['Style'].astype(str)
    return df_filtered


def calculate_capacity(df_production, df_capacity):
    """
    ฟังก์ชันสำหรับคำนวณและจัดสรรการผลิตตาม capacity
    """
    required_prod_cols = ['linkk', 'Issue date', 'Asst', 'Style', 'Qty', 'Zone']
    for col in required_prod_cols:
        if col not in df_production.columns:
            st.error(f"❌ Production Data ที่ถูกกรองแล้วไม่มีคอลัมน์ที่จำเป็น: {col}")
            return pd.DataFrame()

    required_cap_cols = ['linkk', 'Capacity']
    for col in required_cap_cols:
        if col not in df_capacity.columns:
            st.error(f"❌ Capacity Data ไม่มีคอลัมน์ที่จำเป็น: {col}")
            return pd.DataFrame()

    df_merged = pd.merge(df_production, df_capacity, on='linkk', how='left')

    if df_merged['Capacity'].isnull().any():
        st.warning("⚠️ พบค่า 'Capacity' ที่หายไปหลังจากการรวมไฟล์ อาจทำให้การคำนวณไม่สมบูรณ์")

    results_df = pd.DataFrame(columns=['Zone', 'Asst', 'Style', 'Cap_per_shift', 'Day', 'Shift', 'Allocated_Qty', 'linkk'])

    df_merged['Capacity'] = pd.to_numeric(df_merged['Capacity'], errors='coerce')

    for zone, group in df_merged.groupby('Zone'):
        current_day = 1
        remaining_A_capacity = 0
        remaining_B_capacity = 0
        group = group.sort_values(by='Issue date')

        for _, row in group.iterrows():
            asst = row['Asst']
            style = row['Style']
            qty_to_allocate = row['Qty']
            cap_per_shift = row['Capacity']
            linkk = row['linkk']

            if pd.isna(cap_per_shift) or pd.isna(qty_to_allocate):
                continue

            if remaining_A_capacity == 0 and remaining_B_capacity == 0:
                remaining_A_capacity = cap_per_shift
                remaining_B_capacity = cap_per_shift

            while qty_to_allocate > 0:
                if remaining_A_capacity > 0:
                    allocated_qty = min(qty_to_allocate, remaining_A_capacity)
                    new_row = pd.DataFrame([{
                        'Zone': zone,
                        'Asst': asst,
                        'Style': style,
                        'Cap_per_shift': cap_per_shift,
                        'Day': current_day,
                        'Shift': 'A',
                        'Allocated_Qty': allocated_qty,
                        'linkk': linkk
                    }])
                    results_df = pd.concat([results_df, new_row], ignore_index=True)
                    qty_to_allocate -= allocated_qty
                    remaining_A_capacity -= allocated_qty
                
                if qty_to_allocate > 0 and remaining_B_capacity > 0:
                    allocated_qty = min(qty_to_allocate, remaining_B_capacity)
                    new_row = pd.DataFrame([{
                        'Zone': zone,
                        'Asst': asst,
                        'Style': style,
                        'Cap_per_shift': cap_per_shift,
                        'Day': current_day,
                        'Shift': 'B',
                        'Allocated_Qty': allocated_qty,
                        'linkk': linkk
                    }])
                    results_df = pd.concat([results_df, new_row], ignore_index=True)
                    qty_to_allocate -= allocated_qty
                    remaining_B_capacity -= allocated_qty
                
                if qty_to_allocate > 0:
                    current_day += 1
                    remaining_A_capacity = cap_per_shift
                    remaining_B_capacity = cap_per_shift
    
    return results_df


# --- Streamlit UI ---
st.set_page_config(page_title="Production Capacity Calculator", layout="wide")
st.title("เครื่องมือคำนวณและจัดสรร Production Capacity")

st.markdown("""
ยินดีต้อนรับ! อัปโหลดไฟล์ CSV 2 ไฟล์เพื่อคำนวณ:
1.  **Production Data:** ไฟล์ข้อมูลการผลิตหลัก
2.  **Capacity Data:** ไฟล์ข้อมูล Capacity ที่ใช้เชื่อมด้วยคอลัมน์ 'linkk'
""")

uploaded_production_file = st.file_uploader("อัปโหลด Production Data (CSV)", type=["csv"], key="prod")
uploaded_capacity_file = st.file_uploader("อัปโหลด Capacity Data (CSV)", type=["csv"], key="capa")

if uploaded_production_file and uploaded_capacity_file:
    try:
        df_prod_master = pd.read_csv(uploaded_production_file)
        df_capacity = pd.read_csv(uploaded_capacity_file)

        st.success("✅ อัปโหลดไฟล์สำเร็จ!")

        st.subheader("ข้อมูล Production Data (ตัวอย่าง 5 แถวแรก)")
        st.dataframe(df_prod_master.head())

        st.subheader("ข้อมูล Capacity Data (ตัวอย่าง 5 แถวแรก)")
        st.dataframe(df_capacity.head())

        if st.button("เริ่มคำนวณ"):
            with st.spinner("กำลังประมวลผล..."):
                df_filtered = filter_production_data(df_prod_master.copy())
                
                if df_filtered is not None:
                    results_df = calculate_capacity(df_filtered, df_capacity)

                    if not results_df.empty:
                        st.subheader("ผลลัพธ์การคำนวณและจัดสรร Production Capacity")
                        st.dataframe(results_df)

                        st.markdown("---")

                        # แสดงสรุปผลลัพธ์
                        total_allocated = results_df['Allocated_Qty'].sum()
                        total_qty = df_filtered['Qty'].sum()
                        st.markdown(f"**จำนวนรวมที่จัดสรรได้:** {total_allocated:,.0f} units")
                        st.markdown(f"**จำนวนรวมที่ต้องจัดสรร (จากข้อมูลที่กรองแล้ว):** {total_qty:,.0f} units")
                        st.markdown(f"**ความแม่นยำในการจัดสรร:** { (total_allocated / total_qty) * 100 if total_qty > 0 else 0 :.2f}%")

                        # สร้างปุ่มสำหรับดาวน์โหลด
                        csv = results_df.to_csv(index=False).encode('utf-8-sig')
                        st.download_button(
                            label="📥 ดาวน์โหลดไฟล์ผลลัพธ์ (CSV)",
                            data=csv,
                            file_name="calculated_production_capacity.csv",
                            mime="text/csv",
                        )

    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาดในการประมวลผลไฟล์: {e}")
