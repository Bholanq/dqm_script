this is for Alumus
install all dependencies using 
py -m pip install -r requirements.txt

python dqm_script.py \
  --schema           sandbox \
  --ctrl_dqm_master  ctrl_dqm_master \
  --ctrl_dqm_type    ctrl_dqm_type \
  --source           test_dqm_data \
  --quarantine_table dqm_quarantined_records \
  --passed_table     dqm_passed_records \
  --log_table        dqm_log