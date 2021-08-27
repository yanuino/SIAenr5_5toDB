import io
import sqlite3
from sqlite3 import Error
import re

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: connection object or None
    """

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """

    try:
        c= conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def remove(data, start, end):
    """
    :param data: string to process
    :param start: begin of the string to remove
    :param end: end of the string to remove
    """
    deb = 0
    fin = 0
    ssize = len(start)
    esize = len(end)

    while 1:
        deb = data.find(start, deb)
        if deb == -1:
            return data

        fin = deb + ssize
        fin = data.find(end, fin)
        if fin != -1:
            data = data.replace(data[deb:fin + esize],"",1)


def clean_html_enr5_5(file):
    """ cleanning htlm file containing ENR 5.5 from SIA
    :param file: name of the html file 
    :return: cleaned data
    """
    with io.StringIO() as temp:
        with open(file, "r", encoding="utf_8") as file_in:
            line = file_in.readline()
            while line:
                line = line.replace("Visibility=\"None\"","")
                line = line.replace("Page-break=\"None\"","")
                line = line.replace("rowspan=\"1\"","")
                line = line.replace("colspan=\"1\"","")
                
                temp.write(line)
                line = file_in.readline()

        temp_data = temp.getvalue()

    temp_data = remove(temp_data, "<script", "/script>")
    temp_data = remove(temp_data, "<thead", ">")
    temp_data = remove(temp_data, "<input", ">")
    temp_data = remove(temp_data, "<del", "/del>")
    temp_data = remove(temp_data, "<ins", ">")
    
    temp_data = temp_data.replace("</ins>","")
    temp_data = temp_data.replace("<br />","")

    temp_data = temp_data.replace("GEO_LAT-VAL_DIST_VER_LOWER-UOM_DIST_VER_LOWER-GEO_LONG-CODE_DIST_VER_LOWER-TXT_RMK_NAT--1",
                     "GEO_LAT-VAL_DIST_VER_LOWER-UOM_DIST_VER_LOWER-GEO_LONG-TXT_RMK_NAT-CODE_DIST_VER_LOWER--1")

    temp_data = temp_data.replace("UOM_DIST_VER_MAX-CODE_DIST_VER_UPPER-CODE_DIST_VER_MAX-TXT_NAME-VAL_DIST_VER_UPPER-UOM_DIST_VER_UPPER-TXT_RMK_WORK_HR-VAL_DIST_VER_MAX-NOM_USUEL",
                     "CODE_DIST_VER_UPPER-TXT_NAME-VAL_DIST_VER_UPPER-UOM_DIST_VER_UPPER-TXT_RMK_WORK_HR-NOM_USUEL")

    return temp_data

def decode_geo(string, long=0):
    """ decode coordinate from string
    """
    geo = -1

    if long == True:
        expr = r"(^\d+)°(\d+)'(\d+)\"([EW])$"
    else:
        expr = r"(^\d+)°(\d+)'(\d+)\"([NS])$"
    res = re.match(expr, string)
    if res:
        geo_h = float(res[1])
        geo_m = float(res[2])
        geo_s = float(res[3])
        geo = geo_h + geo_m /60.0 + geo_s / 3600.0
        dummy = int(geo*100000.0)
        geo = dummy/100000.0

    return geo

def getrecords(rawdata):
    """ parse data for extract ENR 5.5 records
    :param rawdata: data to parse
    :return: generate records
    """
    l1 = 0
    eol1 = 0
    l2 = 0
    eol2 = 0

    while 1:
        l1 = rawdata.find("CODE_DIST_VER_UPPER-TXT_NAME-VAL_DIST_VER_UPPER-UOM_DIST_VER_UPPER-TXT_RMK_WORK_HR-NOM_USUEL", l1)
        if l1 == -1:
            break

        Id = ""
        Activity = ""
        Name = ""
        Dist_Ver_Upper = ""
        Dist_Ver_Max = ""
        Geo_Lat = ""
        Geo_Long = ""
        Dist_Ver_Lower = ""

        l1 = l1 + len("CODE_DIST_VER_UPPER-TXT_NAME-VAL_DIST_VER_UPPER-UOM_DIST_VER_UPPER-TXT_RMK_WORK_HR-NOM_USUEL")
        eol1 = rawdata.find("</tr>", l1)
        ligne1 = re.sub(' +',' ', rawdata[l1:eol1])
        l2 = rawdata.find("GEO_LAT-VAL_DIST_VER_LOWER-UOM_DIST_VER_LOWER-GEO_LONG-TXT_RMK_NAT-CODE_DIST_VER_LOWER--1",l1)
        if l2 >= 0:
            l2 = l2 + len("GEO_LAT-VAL_DIST_VER_LOWER-UOM_DIST_VER_LOWER-GEO_LONG-TXT_RMK_NAT-CODE_DIST_VER_LOWER--1")
            eol2 = rawdata.find("</tr>", l2)
            ligne2 = re.sub(' +',' ', rawdata[l2:eol2])
        
        deb = 0
        fin = 0
        
        deb = ligne1.find("TXT_NAME", deb)
        deb = ligne1.find(">", deb)
        deb = deb + 1
        fin = ligne1.find("</span", deb)
        Id = re.sub(' +', ' ', ligne1[deb:fin])
        
        deb = ligne1.find("<span", deb)
        deb = ligne1.find(">", deb)
        deb = deb + 1
        fin = ligne1.find("</span", deb)
        Activity = re.sub(' +', ' ', ligne1[deb:fin])

        deb = ligne1.find("NOM_USUEL", deb)
        deb = ligne1.find(">", deb)
        borne = ligne1.find("</td>", deb)
        fin = ligne1.find("</span", deb)
        if fin < borne:
            deb = deb + 1
            Name = re.sub(' +', ' ',ligne1[deb:fin])

        deb = ligne1.find("UOM_DIST_VER_UPPER", deb)
        deb = ligne1.find(">", deb)
        deb = deb + 1
        fin = ligne1.find("</span", deb)
        Dist_Ver_Upper = re.sub(' +', ' ', ligne1[deb:fin])
        
        debmax = ligne1.find("CODE_DIST_VER_MAX", deb)
        if debmax > -1:
            deb = ligne1.find(">", debmax)
            deb = deb + 1
            fin = ligne1.find("</span", deb)
            Dist_Ver_Max = re.sub(' +', ' ', ligne1[deb:fin])

        deb = 0
        fin = 0
        deb = ligne2.find("GEO_LAT", deb)
        deb = ligne2.find(">", deb)
        deb = deb + 1
        fin = ligne2.find("</span", deb)        
        Geo_Lat = re.sub(' +', ' ', ligne2[deb:fin])

        deb = 0
        fin = 0
        deb = ligne2.find("GEO_LONG", deb)
        deb = ligne2.find(">", deb)
        deb = deb + 1
        fin = ligne2.find("</span", deb)        
        Geo_Long = re.sub(' +', ' ', ligne2[deb:fin])

        deb = 0
        fin = 0
        deb = ligne2.find("CODE_DIST_VER_LOWER", deb)
        deb = ligne2.find(">", deb)
        deb = deb + 1
        fin = ligne2.find("</span", deb)        
        Dist_Ver_Lower = re.sub(' +', ' ', ligne2[deb:fin])

        geo_lat = decode_geo(Geo_Lat, False)
        geo_long = decode_geo(Geo_Long, True)
        
        yield (Id, Activity, Name, geo_lat, geo_long, Dist_Ver_Upper, Dist_Ver_Max, Dist_Ver_Lower)

def main():
    database = r".\enr5_5.db"

    sql_create_enr_table = """ CREATE TABLE IF NOT EXISTS enr(
                                    id TEXT UNIQUE NOT NULL,
                                    activity TEXT,
                                    name TEXT,
                                    geo_lat REAL,
                                    geo_long REAL,
                                    dist_ver_upper TEXT,
                                    dist_ver_max TEXT,
                                    dist_ver_lower TEXT
                                    ); """

    sql_insert_enr = """ INSERT INTO enr(
                            id,
                            activity,
                            name,
                            geo_lat,
                            geo_long,
                            dist_ver_upper,
                            dist_ver_max,
                            dist_ver_lower
                            ) VALUES(?,?,?,?,?,?,?,?) """

    conn = create_connection(database)

    if conn is not None:
        create_table(conn, sql_create_enr_table)

    rawdata = clean_html_enr5_5("test.html")
    with open("temp.html", "w", encoding="utf_8") as temp:
        temp.write(rawdata)
    
    cur = conn.cursor()
    
    records = getrecords(rawdata)
    for enr in records:
        print(enr)
        try:
            cur.execute(sql_insert_enr, enr)
        except sqlite3.Error as error:
            print("Failed to insert into sqlite table", error, enr[0])

    conn.commit()
    conn.close()    

if __name__ == "__main__":
    main()


    