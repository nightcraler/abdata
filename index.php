<?php


//Start
$start= microtime(true);
echo 'Start: '.$start.'<br /><br />';

//Einbinden der Configuration
include('AdditionalConfiguration.php');
//Datenbank Link
$db_link = $GLOBALS['dblink'];


//Arrays mit den zu bearbeitenden Dateien
$checkfilearray = array();

$checkfilearray[] = '/ADR_APO.GES';
$checkfilearray[] = '/DAR_APO.GES';
$checkfilearray[] = '/PGR_APO.GES';
$checkfilearray[] = '/PGR2_APO.GES';
$checkfilearray[] = '/WAR_APO.GES';
$checkfilearray[] = '/PAC_APO.GES';

$checkfilearray[] = '/ADR_APO.UPD';
$checkfilearray[] = '/DAR_APO.UPD';
$checkfilearray[] = '/PGR_APO.UPD';
$checkfilearray[] = '/PGR2_APO.UPD';
$checkfilearray[] = '/WAR_APO.UPD';
$checkfilearray[] = '/PAC_APO.UPD';

//Einlesen des Verzeichnisses im Basisordner
$dir = scandir(BASE_FOLDER, 1);

//echo var_dump($dir);

//Durchlaufen des Arrays mit den zu bearbeitenden Dateien
foreach($checkfilearray as $checkfile) {

    if (file_exists(BASE_FOLDER.'/'.$dir[0].$checkfile)) {

        //Startzeit der Datei
        $starteinzel = microtime(true);

        //Handler auf die Datei
        $chIfa = fopen(BASE_FOLDER . '/' . $dir[0] . $checkfile, "r");


        //Tabellendaten einlesen (Tabellenname, UPD oder GES und Startzeit
        $tablename = getTablename($chIfa);

        //Felddatenarray erstellen
        $fieldarray = createFieldArray($chIfa);



        //Tabellen anlegen wenn nicht vorhanden
        createTable($tablename[0], $db_link, $fieldarray);


        //Abfrage ob Update oder Gesamt
        if ($tablename[1] == 'GES') {

            //Schnelle abarbeitung der Inputs
            $dataarray = createshortdataarray($chIfa, $tablename, $db_link, $fieldarray);

        }
        if ($tablename[1] == 'UPD') {
            //Erstellung eines Arrays mit Insert, Update und Delete
            $dataarray = createdataarray($chIfa, $tablename[0], $db_link, $fieldarray);

            if ($dataarray[0]) {
                insertDataSingle($dataarray[0], $tablename, $db_link, $fieldarray);
            }

            if ($dataarray[1]) {
                updateDataSingle($dataarray[1], $tablename, $db_link, $fieldarray);
            }
            if ($dataarray[2]) {
                setDeleteDataSingle($dataarray[2], $tablename, $db_link, $fieldarray);
            }


        }


        //Anzeige der Laufzeit einer Datei
        $dauereinzel = microtime(true) - $starteinzel;
        echo "Verarbeitung der Datei: $dauereinzel Sek. <br /><br />";

    }

}

//Verschieben der Dateien zu Done
if(count($dir) > 2) {
    rename(BASE_FOLDER.'/'.$dir[0],DONE_FOLDER.'/'.$dir[0]);
}


//Anzeige der Gesamtlaufzeit
$dauer = microtime(true) - $start;
echo "Verarbeitung des Skripts: $dauer Sek. <br /><br />";


//Lösungsvermerk im letzten Eintrag einfügen
function setDeleteDataSingle($dataarray, $tablename, $db_link, $fieldarray) {



    foreach ($dataarray as $datas) {

        $lastselect = "SELECT * FROM " . $tablename[0] . " WHERE ".$fieldarray[0][1]." = " . $datas['01']." ORDER BY UID DESC LIMIT 1";


        if ($result = $db_link->query($lastselect)) {
            //echo "Import erfolgreich";
        } else {
            echo "Fetch nicht erfolgreich: " . $db_link->error . '</br>';
        }

        $row = $result->fetch_assoc();


        $start = "UPDATE " . $tablename[0]." ";
        $start .= "SET DELETED = 1, TIME_DELETED = ".$tablename[2]." ";
        $start .= "WHERE UID = " . $row['UID'];




        if ($db_link->query($start) === TRUE) {
            //echo "Import erfolgreich";
        } else {
            echo "DELETE nicht erfolgreich: " . $db_link->error . '</br>';
        }

    }






}


//Erstellen eines kompletten Datensatzes mit den Werten des letzten Eintrages und den Updatedaten
//Kann schnell geändert werden do das nur noch die Änderugen in einem Datensatz stehen
function updateDataSingle($dataarray, $tablename, $db_link, $fieldarray) {



    $start = "INSERT INTO ".$tablename[0]." (";

    $i = 1;
    foreach ($fieldarray as $field) {
        $start .= $field[1];
        if($i < count($fieldarray)) {
            $start .= ' , ';
        }
        $i++;
    }
    $start .= ' , TIME_START ) VALUES ';
    $d = 1;
    $u = 1;

    foreach ($dataarray as $datas) {

        $lastselect = "SELECT * FROM ".$tablename[0]." WHERE ".$fieldarray[0][1]." = ".$datas['01']." ORDER BY UID DESC LIMIT 1";




        if ($result = $db_link->query($lastselect)) {
            //echo "Import erfolgreich";
        } else {
            echo "Fetch nicht erfolgreich: " . $db_link->error.'</br>';
        }

        $row = $result->fetch_array();



        $select = $start;
        $select .= '( ';


        foreach ($fieldarray as $fieldkey => $field) {





            $fieldmatch = false;

            foreach ($datas as $index => $data) {


                if(strval($index) == strval($field[0])) {


                    $select .= "'";

                    $stringdata = protoreplace($data);
                    $select .= $db_link->real_escape_string($stringdata);
                    if($u < count($fieldarray)) {
                        $select .= "' ,";
                    }
                    else {
                        $select .= "'";
                    }
                    $u++;
                    $fieldmatch = true;
                }


            }
            if($fieldmatch == false) {

                //echo $row[$fieldkey];

                if($row[$fieldkey+1] != NULL){
                    $select .= "'".$row[$fieldkey+1]."'";
                }
                else {
                    $select .= "NULL";
                }


                //$select .= "NULL";
                if($u < count($fieldarray)) {
                    $select .= " ,";
                }

                $u++;
            }






        }
        $select .= " , ".$tablename[2];

        $select .= ') ';
        if($d < count($dataarray)) {
            //$select .= ' , ';
        }
        $u = 1;
        $d++;



        if ($db_link->query($select) === TRUE) {
            //echo "Import erfolgreich";
        } else {
            echo '<pre>';
            echo "Import nicht erfolgreich: " . $db_link->error.'</br>';
            echo "Query: ".$select;
            echo '<pre>';
        }




    }
    //$select .= ')';




}

//Erstellt drei Arrays für Insert, Update und Delete aus den Updatedateien
//Fügt diese danach in einem Array zusammen
function createdataarray($chIfa) {

    //$nexttable = 0;
    //$nextfield = 0;
    $nextdata = 0;
    $nextupdate = 0;
    $nextdelete = 0;
    $dataarray = array();
    $i = -1;
    $allarray = array();

    while(($rowCsvIfa = fgets($chIfa)) !== FALSE) {
        $identifier = substr($rowCsvIfa, 0, 2);
        $value = substr($rowCsvIfa, 2, -2);

        if ($identifier == '00') {
            $datacounter = 0;
            $nextdata = 0;
            $nextupdate = 0;
            $nextdelete = 0;

            if ($value == "I") {
                $nextdata = 1;


                $i++;
            }
            if($value == "D") {
                $nextdelete = 1;
                $i++;

            }
            if($value == "U") {
                $nextupdate = 1;
                $i++;

            }

            if($value == "E") {
                break;

            }

        }

        if($nextdata == 1 && $identifier != '00') {
            $dataarray[$i][$identifier] = $value;

            $datacounter ++;
        }

        if($nextdelete == 1 && $identifier != '00') {
            $deletearray[$i][$identifier] = $value;
            $datacounter ++;
        }

        if($nextupdate == 1 && $identifier != '00') {

            //
            $updatearray[$i][$identifier] = $value;
            $datacounter ++;
        }


    }

    $allarray[0] = $dataarray;
    $allarray[1] = $updatearray;
    $allarray[2] = $deletearray;

    return $allarray;
}


//Verarbeitet die Inserts bei GES Dateien
//Nach 5000 Einträgen im Array wird Insert ausgeführt
function createshortdataarray($chIfa,$tablename,$db_link,$fieldarray) {

    //$nexttable = 0;
    //$nextfield = 0;
    $nextdata = 0;
    //$nextupdate = 0;
    //$nextdelete = 0;

    $dataset = 0;
    $break = 0;
    $dataarray = array();
    $i = -1;
    $allarray = array();
    $counter = 1;

    $datainsert = 0;
    while(($rowCsvIfa = fgets($chIfa)) !== FALSE) {
        $identifier = substr($rowCsvIfa, 0, 2);
        $value = substr($rowCsvIfa, 2, -2);



        if ($identifier == '00') {
            $datacounter = 0;
            $nextdata = 0;

            if ($value == "I") {
                $nextdata = 1;

                $dataset = 1;
                //echo '<pre>';
                //echo var_dump($value);
                //echo '</pre>';
                $i++;
            }
            else if($value == "F") {
                $break = 0;
            }
            else {
               $break = 1;
            }

            if(count($dataarray) == 5000 && $nextdata == 1) {



                insertDataSingle($dataarray,$tablename,$db_link,$fieldarray);
                //$counter = 0;
                $dataarray = null;
                $datainsert++;
            }

        }


        if($nextdata == 1 && $identifier != '00') {
            $dataarray[$i][$datacounter] = $value;


            $datacounter++;
        }
        //$counter++;

        if($break == 1 && $dataset == 1) {

        }




    }
    //Insert für die letzte Arrayrunde
    insertDataSingle($dataarray,$tablename,$db_link,$fieldarray);
}


//erstellen des Tablearrays
//Tabellename, Dateiart, Startzeit
function getTablename($chIfa) {

    $nexttable = 0;

    $tablevalue = array();

    while(($rowCsvIfa = fgets($chIfa)) !== FALSE) {
        $identifier = substr($rowCsvIfa, 0, 2);
        $value = substr($rowCsvIfa,2,-2);

        if($identifier == '00') {


            if($value == "K"){
                $nexttable = 1;
            }

        }

        if($nexttable == 1 && $identifier == '01' ) {

            $tablevalue[] = $value;

        }
        if($nexttable == 1 && $identifier == '02' ) {

            $tablevalue[] = $value;

        }
        if($nexttable == 1 && $identifier == '03' ) {

            $tablevalue[] = $value;
            return $tablevalue;

        }

    }

}


//Erstellt Array mit Feldinformationen
function createFieldArray($chIfa) {
    $fielarray = array();
    $insertcounter = 0;
    $fieldset = 0;
    $break = 0;
    $fieldindex = 0;

    //$nexttable = 0;
    $nextfield = 0;


    while(($rowCsvIfa = fgets($chIfa)) !== FALSE) {
        $identifier = substr($rowCsvIfa, 0, 2);
        $value = substr($rowCsvIfa,2,-2);

        if($identifier == '00') {
            $nextfield = 0;

            if($value == "F"){
                $nextfield = 1;
                $insertcounter++;
                $fieldset = 1;
            }
            else {
                $break = 1;
            }
        }

        if($nextfield == 1 && $identifier == '01') {
            $fielarray[$fieldindex][0] = $value;
            //$fieldindex++;
        }

        if($nextfield == 1 && $identifier == '02') {
            $fielarray[$fieldindex][1] = $value;
            //$fieldindex++;
        }

        if($nextfield == 1 && $identifier == '03') {
            $fielarray[$fieldindex][2] = $value;
            //$fieldindex++;
        }
        if($nextfield == 1 && $identifier == '04') {
            $fielarray[$fieldindex][3] = $value;
            //$fieldindex++;
        }
        if($nextfield == 1 && $identifier == '05') {
            $fielarray[$fieldindex][4] = $value;
            //$fieldindex++;
        }
        if($nextfield == 1 && $identifier == '06') {
            $fielarray[$fieldindex][5] = $value;
            //$fieldindex++;
        }

        if($nextfield == 1 && $identifier == '07') {
            $fielarray[$fieldindex][6] = $value;
            $fieldindex++;
        }

        if($break == 1 && $fieldset == 1) {
            return $fielarray;
        }
    }
}


//Ersetzt dir Prototypen durch Zeichen
function protoreplace($text) {
    $search  = array('\A22', '\a22', '\A23', '\a23', '\A24' , '\a24' , '\A25' , '\a25' , '\A29' , '\a29' , '\a33' , '\A45' , '\a45' , '\a63' , '\b63' , '\c22' , '\C27' , '\c27' , '\C49' , '\c49' , '\D27' , '\d27' , '\D63' , '\d63' , '\E22' , '\e22' , '\E23' , '\e23' , '\E24'  , '\e24'  , '\E25' , '\e25' , '\E27' , '\e27' , '\E33' , '\E66' , '\e63' , '\F63' , '\f63' , '\G32' , '\g32' , '\G63' , '\g63' , '\H63' , '\h63' , '\I22' , '\i22' , '\I23' , '\i23' , '\I24' , '\i24' , '\I25'  , '\i25' , '\I36'  , '\i36' , '\I45' , '\i45' , '\i63' , '\J63' , '\j63' , '\k63' , '\L63' , '\l63' , '\M33'  , '\m33' , '\m63' , '\N26' , '\n26' , '\N27' , '\n27' , '\n63' , '\O22' , '\o22' , '\O23' , '\o23' , '\O24' , '\o24' , '\O25' , '\o25' , '\O35' , '\o35' , '\O42' , '\o42' , '\O45' , '\o45' , '\o63' , '\P63' , '\p63' , '\Q63' , '\q63' , '\R27' , '\r27' , '\r63' , '\S27' , '\s27' , '\S39' , '\s39' , '\S49' , '\s49' , '\S63' , '\s63' , '\T27' , '\t27' , '\t63' , '\U22' , '\u22' , '\U23' , '\u23' , '\U24' , '\u24' , '\U25' , '\u25' , '\U29' , '\u29' , '\u63' , '\W63' , '\w63' , '\x63' , '\Y22' , '\y22' , '\Y25' , '\y25' , '\Y63' , '\y63' , '\Z27' , '\z27' , '\z63' , '\321' , '\323' , '\324' , '\325' , '\326' , '\327' , '\328' , '\329' , '\330' , '\333' , '\340' , '\341' , '\344' , '\345' , '\346' , '\347' , '\348' , '\351' , '\360' , '\361' , '\362' , '\363' , '\364' , '\365' , '\367' , '\372' , '\375' , '\380' , '\420' , '\421' , '\422' , '\423' , '\424' , '\425' , '\426' , '\427' , '\428' , '\429' , '\430' , '\431' , '\432' , '\435' , '\460' , '\462' , '\463' , '\465' , '\466' , '\467' , '\473' , '\520' , '\521' , '\535' , '\565' , '\900');
    $replace = array('Á', 'á', 'À', 'à', 'Â', 'â', 'Ä', 'ä', 'Å', 'å', '', 'Æ', 'æ', 'α', 'β', 'ć', 'Č', 'č', 'Ç', 'ç', 'Ď', 'ď', 'Δ', 'δ', 'É', 'é', 'È', 'è', 'Ê', 'ê', 'Ë', 'ë', 'Ě', 'ě', 'Ē', 'Ε', 'ε', 'Φ', 'φ', 'Ǧ', 'ǧ', 'Γ', 'γ', 'Θ', 'θ', 'Í', 'í', 'Ì', 'ì', 'Î', 'î', 'Ï', 'ï', 'i', 'i', 'IJ', 'ij', 'ι', 'Η', 'η', 'κ', 'Λ', 'λ', 'M', 'm', 'μ', 'N', 'n', 'Ň', 'ň', 'ν', 'Ó', 'ó', 'Ò', 'ò', 'Ô', 'ô', 'Ö', 'ö', 'Ő', 'ő', 'Ø', 'ø', 'Œ', 'œ', 'ο', 'Π', 'π', 'Ξ', 'ξ', 'Ř', 'ř', 'ρ', 'Š', 'š', 'ß', 'ß', 'Ş', 'ş', 'Σ', 'σ', 'Ť', 'ť', 'τ', 'Ú', 'ú', 'Ù', 'ù', 'Û', 'û', 'Ü', 'ü', 'Ů', 'Ů', 'υ', 'Ω', 'ω', 'χ', 'Ý', 'ý', 'Ÿ', 'ÿ', 'Ψ', 'ψ', 'Ž', 'ž', 'ζ', ',', ':', '!', '?', '-', '=', '/', '’', '‘', '\\', '⋅', '|', '′', '′′', '-', '°', '‰', '≙', '+', '&', '*', '§', '%', ' ', '$', '@', '×', '®', '(', ')', '[', ']', '{', '}', '<', '>', '«', '»', '"', '"', '→', '↑', '±', '∞', '↔', '~', '≃', '≅', 'ø', '≤', '≥', '↓', '≈', '€');



    return str_replace($search, $replace, $text);
}

//Erstellt die Tabellen wenn sie noch nicht existieren
function createTable($tablename, $db_link, $fieldarray) {


    $sql = "CREATE TABLE IF NOT EXISTS ".$tablename." (";
    $i = 1;
    $u = 1;
    $primarykeys = array();
    $sql .= "UID int NOT NULL AUTO_INCREMENT , ";
    foreach ($fieldarray as $field) {


        if($field[6] == 'AL1' || $field[6] == 'AN1' || $field[6] == 'FN1' || $field[6] == 'FN2' || $field[6] == 'GRU' || $field[6] == 'ID1' || $field[6] == 'IND' || $field[6] == 'MPG' || $field[6] == 'NU3' || $field[6] == 'PRO' || $field[6] == 'WGS') {
            $feldtyp = 'varchar';
        }
        if($field[6] == 'DT8' || $field[6] == 'DTV' || $field[6] == 'IKZ' || $field[6] == 'NU1' || $field[6] == 'NU2' || $field[6] == 'ONH' || $field[6] == 'PZN' || $field[6] == 'PZ8') {
            $feldtyp = 'int';
        }
        if($field[6] == 'FLA') {
            $feldtyp = 'bool';
        }
        if($field[6] == 'GK1') {
            $feldtyp = 'float';
        }

        //Datentyp
        //AL = Varchar
        //AN = Varchar
        //B64 = base64
        //DT8 = Datum 8stellig
        //DTV = Datum 6stellig
        //FLA = bool
        //FN1 = varchar
        //FN2 = varchar
        //GK1 = float
        //GRU = varchar
        //ID1 = varchar
        //IKZ = INT
        //IND = varchar
        //MPG = varchar
        //NU1 = INT
        //NU2 = INT
        //NU3 = varchar
        //ONH = INT
        //PRO = varchar
        //PZN = INT
        //PZ8 = INT
        //WGS = varchar



        //Feldlänge
        // F = feste länge
        // V = variable länge id6 = max
        // U = variable unbegrenzte länge

        //$field[0] = Identifier Zähler
        //$field[1] = Spaltenname
        //$field[2] = Primärschlüssel
        //$field[3] = Null erlaubt?
        //$field[4] = Feldlängentyp s.o
        //$field[5] = Feldlänge
        //$field[6] = Datentyp




        if($field[2] == 1) {
            //$sql .= $field[1]." VARBINARY(1000)";

            $primarykeys[] = $field[1];


        }
        else {
            //$sql .= $field[1]." TEXT";
        }
        if($feldtyp == 'varchar'){
            if($field[4] == 'U'){
                $sql .= $field[1]." TEXT";
            }
            else {
                $sql .= $field[1]." VARCHAR(".$field[5].")";
            }

        }
        if($feldtyp == 'int'){
            $sql .= $field[1]." int(".$field[5].")";
        }
        if($feldtyp == 'float'){
            $sql .= $field[1]." float(".$field[5].")";
        }
        if($feldtyp == 'bool'){
            $sql .= $field[1]." smallint(".$field[5].")";
        }
        if($i < count($fieldarray)) {

        }
        $sql .= ' , ';
        $i++;

    }
    $sql .= ' TIME_START int(12) ,';
    $sql .= ' TIME_END int(12) ,';
    $sql .= ' DELETED int(5) ,';
    $sql .= ' TIME_DELETED int(12) ,';
    $sql .= ' PRIMARY KEY ( UID , ';
    foreach ($primarykeys as $key) {
        $sql .= $key;
        if($u < count($primarykeys)) {
            $sql .= ' , ';
        }

        $u++;
    }
    $sql .= ")";
    $sql .= ")";




    if ($db_link->query($sql) === TRUE) {
        echo "Table created successfully: ".$tablename.'<br /><br />';
    } else {
        echo "Error creating table: " . $db_link->error.'<br /><br />';
    }



}

//Insert in Datenbank einfügen
function insertDataSingle($dataarray, $tablename, $db_link, $fieldarray) {


    $start = "INSERT INTO ".$tablename[0]." (";

    $i = 1;
    foreach ($fieldarray as $field) {


        $start .= $field[1];
        if($i < count($fieldarray)) {
            $start .= ' , ';
        }
        $i++;
    }
    $start .= ', TIME_START) VALUES ';
    $d = 1;
    $u = 1;
    $select = $start;
    foreach ($dataarray as $datas) {
        $select = $start;
        $select .= '( ';

        foreach ($datas as $data) {


            if($data == "") {
                //echo "Null";
                $select .= "NULL";
            }
            else {
                $select .= "'";

                $stringdata = protoreplace($data);
                $select .= $db_link->real_escape_string($stringdata);
            }


            if($data == "") {
                //echo "Null";

                if($u < count($datas)) {
                    $select .= " ,";
                }
                else {
                    //$select .= "";
                }
                $u++;
            }else {
                if($u < count($datas)) {
                    $select .= "' ,";
                }
                else {
                    $select .= "'";
                }
                $u++;
            }

        }
        $select .= " , ".$tablename[2];
        $select .= ') ';
        if($d < count($dataarray)) {
            //$select .= ' , ';
        }
        $u = 1;
        $d++;




        if ($db_link->query($select) === TRUE) {
            //echo "Import erfolgreich";
        } else {
            echo "Import nicht erfolgreich: " . $db_link->error.'</br>';
            echo '<pre>';
            echo $select;
            echo '</pre>';

        }

    }
    //$select .= ')';

    //if ($db_link->query($select) === TRUE) {
        //echo "Import erfolgreich";
    //} else {
    //    echo "Import nicht erfolgreich: " . $db_link->error.'</br>';
    //}






}
