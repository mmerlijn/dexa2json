import pyodbc
import json
import os
import tkinter as tk
from datetime import date, datetime

if not os.path.isfile('config.json'):
    f = open("config.json", "w")
    config_ = {'mdb_file_path': "c:/qdr/data/patscan.mdb",  # K:/python/mdb/Testexport150621.mdb
               'export_file_path': "c:/export.txt",  # K:/python/mdb/export.txt
               'from': "2021-01-01"}
    f.write(json.dumps(config_))
    f.close()
# inladen van configuratie gegevens, zodat bij aanpassen dit niet steeds hoeft te worden gewijzigd
config_tmp = json.load(open('config.json'))

root = tk.Tk()
root.wm_title("Dexa export to json")
tk.Label(root, text="MDB file path").grid(row=0)
tk.Label(root, text="Export file path").grid(row=1)
tk.Label(root, text="Vanaf datum").grid(row=2)
input1 = tk.Entry(root, width=100)
input2 = tk.Entry(root, width=100)
input3 = tk.Entry(root, width=100)
input1.grid(row=0, column=1)
input2.grid(row=1, column=1)
input3.grid(row=2, column=1)
input1.insert(10, config_tmp['mdb_file_path'])
input2.insert(10, config_tmp['export_file_path'])
input3.insert(10, config_tmp['from'])
tk.Label(root, text="").grid(row=4, column=0)


def script():
    tk.Label(root, text="").grid(row=2, column=1)
    # save path to config.json
    f3 = open("config.json", "w")
    config = {'mdb_file_path': input1.get(),
              'export_file_path': input2.get(),
              'from': input3.get()
              }
    f3.write(json.dumps(config))
    f3.close()

    # make a list of all odbc drivers
    msaccess_drivers = [x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')]

    # ODBC access driver aanwezig test
    if not len(msaccess_drivers):
        tk.Label(root, text="Geen access drivers aanwezig", font=('helvetica', 12, 'bold'), fg='red').grid(row=3,
                                                                                                           column=1)
        return
    if not os.path.isfile(config['mdb_file_path']):
        tk.Label(root, text="Bestand " + config['mdb_file_path'] + " is niet aanwezig!",
                 font=('helvetica', 12, 'bold'), fg='red').grid(row=3, column=1)
        return
    # proberen verbinding te maken met de MDB database
    try:
        conn = pyodbc.connect("Driver={" + msaccess_drivers[0] + "};DBQ=" + config[
            'mdb_file_path'] + ";")  # K:/python/mdb/Testexport150621.mdb
    except:
        tk.Label(root, text="Kan geen connectie met de database " + config['mdb_file_path'] + " maken.",
                 font=('helvetica', 12, 'bold'), fg='red').grid(row=3, column=1)
        return

    # Alle patient_id's ophalen die vanaf de betreffende datum zijn gescand
    scans_cursor = conn.cursor()
    scans_cursor.execute(
        "select distinct PATIENT_KEY from ScanAnalysis where SCAN_DATE >= #" + datetime.strptime(config['from'],
                                                                                                 "%Y-%m-%d").strftime(
            "%m/%d/%Y") + "#")
    data = []  # we doorlopen alle patienten die na de betreffende datum zijn gescand en zetten dit in `data`
    for scans in scans_cursor.fetchall():
        cursor = conn.cursor()
        cursor.execute(
            "select PATIENT_KEY,PATIENT.LAST_NAME,PATIENT.BIRTHDATE, PATIENT.SEX, PATIENT.HEIGHT, PATIENT.WEIGHT, PATIENT.ETHNICITY, IDENTIFIER1, IDENTIFIER2 from PATIENT WHERE PATIENT_KEY='" +
            scans[0] + "'")

        for row in cursor.fetchall():
            rij = {'naam': row[1], 'gebdat': row[2].strftime("%Y-%m-%d"), 'sexe': row[3], 'lengte': row[4],
                   'gewicht': row[5],
                   'etniciteit': row[6], 'identifier1': row[7], 'identifier2': row[8]}
            cursor2 = conn.cursor()
            cursor2.execute(
                "select Hip.SCANID, NECK_AREA,NECK_BMC,TROCH_AREA, TROCH_BMC, INTER_AREA, INTER_BMC,HTOT_AREA, Hip.HTOT_BMC,Hip.ROI_TYPE,ScanAnalysis.SCAN_DATE,ScanAnalysis.K,ScanAnalysis.D0 from Hip, ScanAnalysis WHERE Hip.SCANID = ScanAnalysis.SCANID and Hip.PATIENT_KEY='" +
                row[0] + "' AND ScanAnalysis.SCAN_DATE >= #" + datetime.strptime(config['from'], "%Y-%m-%d").strftime(
                    "%m/%d/%Y") + "# order by ScanAnalysis.SCAN_DATE desc")
            hip = cursor2.fetchone()
            if hip:
                rij['neckarea'] = hip[1]
                rij['neckbmc'] = hip[2]
                rij['trocharea'] = hip[3]
                rij['trochbmc'] = hip[4]
                rij['interarea'] = hip[5]
                rij['interbmc'] = hip[6]
                rij['totalarea'] = hip[7]
                rij['totalbmc'] = hip[8]
                if hip[9] == 3:
                    rij['heuplr'] = "R"
                elif hip[9] == 2:
                    rij['heuplr'] = "L"
                rij['scandatum'] = hip[10].strftime("%Y-%m-%d")
                rij['heup_k_waarde'] = hip[11]
                rij['heup_d_waarde'] = hip[12]
            cursor2.execute(
                "select Spine.SCANID, L1_AREA, L1_BMC, L2_AREA, L2_BMC, L3_AREA, L3_BMC, L4_AREA, L4_BMC, ScanAnalysis.K, ScanAnalysis.D0 from Spine, ScanAnalysis WHERE Spine.SCANID = ScanAnalysis.SCANID and Spine.PATIENT_KEY='" +
                row[0] + "' and ScanAnalysis.SCAN_DATE >= #" + datetime.strptime(config['from'], "%Y-%m-%d").strftime(
                    "%m/%d/%Y") + "# order by ScanAnalysis.SCAN_DATE desc")
            spine = cursor2.fetchone()
            if spine:
                rij['l1area'] = spine[1]
                rij['l1bmc'] = spine[2]
                rij['l2area'] = spine[3]
                rij['l2bmc'] = spine[4]
                rij['l3area'] = spine[5]
                rij['l3bmc'] = spine[6]
                rij['l4area'] = spine[7]
                rij['l4bmc'] = spine[8]
                rij['spine_k_waarde'] = spine[9]
                rij['spine_d_waarde'] = spine[10]
            data.append(rij)
    try:
        f2 = open(config['export_file_path'], "w")  # "K:/python/mdb/export.txt"
        f2.write(json.dumps(data))
        f2.close()
        tk.Label(root, text="Export is aangemaakt in " + config['export_file_path'],
             font=('helvetica', 12, 'bold'), fg='green').grid(row=3, column=1)
    except:
        tk.Label(root, text="Geen schrijfrechten voor " + config['export_file_path'],
             font=('helvetica', 12, 'bold'), fg='red').grid(row=3, column=1)
    return


tk.Button(root, text='Export to file', command=script).grid(row=3, column=0, sticky=tk.W,
                                                            pady=5, padx=5)

tk.Label(root, text="Copyright Menno Merlijn Software Solutions 2021").grid(row=7, column=1)
root.mainloop()
