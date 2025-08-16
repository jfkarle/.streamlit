def show_scheduler_page():
    st.title("ECM Logistics")

    # --- UI: Customer Selection ---
    selected_customer = st.selectbox("Selected Customer:", [b.customer_name for b in ecm.LOADED_BOATS.values()], key="customer_select")

    if selected_customer:
        boat = next((b for b in ecm.LOADED_BOATS.values() if b.customer_name == selected_customer), None)
    else:
        boat = None

    # --- Display boat data ---
    if boat:
        st.markdown("**Boat Details:**")
        st.markdown(f"- **Type:** {boat.boat_type}")
        st.markdown(f"- **Length:** {boat.boat_length}’")
        st.markdown(f"- **Draft:** {boat.boat_draft}’")
        st.markdown(f"- **Preferred Ramp:** {boat.preferred_ramp}")

        # --- Service Type ---
        selected_service_type = st.selectbox("Service Type:", ["Launch", "Haul", "Land-Land"])

        # --- Requested Date ---
        requested_date = st.date_input("Requested Date:", datetime.date.today())

        # --- Ramp Options ---
        ramp_options = list(set([r.ramp_name for r in ecm.LOADED_RAMPS.values()]))
        selected_ramp_name = st.selectbox("Ramp:", ramp_options)

        all_ramps = list(ecm.LOADED_RAMPS.values())
        selected_ramp = next((r for r in all_ramps if r.ramp_name == selected_ramp_name), None)

        if selected_ramp:
            all_trucks = list(ecm.LOADED_TRUCKS.values())
            all_settings = ecm.GLOBAL_SETTINGS
            try:
                slot_results = ecm.find_available_job_slots(
                    boat=boat,
                    ramp=selected_ramp,
                    date=requested_date,
                    trucks=all_trucks,
                    settings=all_settings
                )

                if slot_results:
                    st.success("Available slots found!")
                    for slot in slot_results:
                        st.write(slot)
                else:
                    st.warning("No slots found.")

            except Exception as e:
                st.error(f"Error occurred while finding slots: {e}")
        else:
            st.error("Selected ramp could not be matched.")

    else:
        st.warning("Please select a customer.")
