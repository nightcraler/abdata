<?php


$start= microtime(true);
echo 'Start';

include('AdditionalConfiguration.php');
$db_link = $GLOBALS['dblink'];



$checkfilearray = array();

$checkfilearray[] = '/ADR_APO.GES';
$checkfilearray[] = '/DAR_APO.GES';
$checkfilearray[] = '/PGR_APO.GES';
$checkfilearray[] = '/PGR2_APO.GES';
$checkfilearray[] = '/WAR_APO.GES';
$checkfilearray[] = '/PAC_APO.GES';





foreach($checkfilearray as $checkfile) {
    $starteinzel= microtime(true);

    $chIfa = fopen(__DIR__.$checkfile, "r");

    $tablename = getTablename($chIfa);

    $fieldarray = createFieldArray($chIfa);


    //echo '<pre>';
    //echo var_dump($fieldarray);
    //echo '</pre>';

    createTable($tablename[0],$db_link,$fieldarray);


    //echo 'rest';

    if($tablename[1] == 'GES'){
        echo 'bla';
        $dataarray = createshortdataarray($chIfa);
        echo 'blu';
    }
    if($tablename[1] == 'UPD'){
        $dataarray = createdataarray($chIfa);
    }

    //echo '<pre>';
    //echo var_dump($dataarray);
    //echo '</pre>';



    if($dataarray[0]) {
        insertDataSingle($dataarray[0],$tablename[0],$db_link,$fieldarray);
    }
    /*
    if($dataarray[1]) {
        //updateDataSingle($dataarray[1],$tablename[0],$db_link,$fieldarray);
    }
    if($dataarray[2]) {
        //setDeleteDataSingle($dataarray[2],$tablename[0],$db_link,$fieldarray);
    }

    */

    $dauereinzel = microtime(true) - $starteinzel;
    echo "Verarbeitung der Datei: $dauereinzel Sek.";

}

$dauer = microtime(true) - $start;
echo "Verarbeitung des Skripts: $dauer Sek.";

function updateDataSingle($dataarray, $tablename, $db_link, $fieldarray) {




    $start = "INSERT INTO ".$tablename." (";

    $i = 1;
    foreach ($fieldarray as $field) {
        $start .= $field[1];
        if($i < count($fieldarray)) {
            $start .= ' , ';
        }
        $i++;
    }
    $start .= ') VALUES ';
    $d = 1;
    $u = 1;
    $select = $start;
    foreach ($dataarray as $datas) {

        $lastselect = "SELECT * FROM ".$tablename." WHERE  ORDER BY id DESC LIMIT 1";

        $result = $db_link->query($lastselect);



        $select = $start;
        $select .= '( ';
        //echo var_dump($datas);
        //$select .= $data;
        foreach ($datas as $data) {
            //echo '<pre>';
            //echo var_dump($data);
            //echo '</pre>';

            $select .= "'";
            //$select .= mysql_real_escape_string($data);
            //$firstkey = array_key_first($data);
            //$select .= $db_link->real_escape_string($data[$firstkey]);
            $select .= $db_link->real_escape_string($data);
            if($u < count($datas)) {
                $select .= "' ,";
            }
            else {
                $select .= "'";
            }
            $u++;
        }

        $select .= ') ';
        if($d < count($dataarray)) {
            $select .= ' , ';
        }
        $u = 1;
        $d++;




    }
    //$select .= ')';

    //echo '<pre>';
    //echo $select;
    //echo '</pre>';

    if ($db_link->query($select) === TRUE) {
        //echo "Import erfolgreich";
    } else {
        echo "Import nicht erfolgreich: " . $db_link->error.'</br>';
    }


}

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

        }

        if($nextdata == 1 && $identifier != '00') {
            $dataarray[$i][$datacounter] = $value;

            $datacounter ++;
        }

        if($nextdelete == 1 && $identifier != '00') {
            $deletearray[$i][$datacounter] = $value;
            $datacounter ++;
        }

        if($nextupdate == 1 && $identifier != '00') {

            //
            $updatearray[$i][$datacounter] = $value;
            $datacounter ++;
        }


    }

    $allarray[0] = $dataarray;
    $allarray[1] = $updatearray;
    $allarray[2] = $deletearray;

    return $allarray;
}



function createshortdataarray($chIfa) {

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

    while(($rowCsvIfa = fgets($chIfa)) !== FALSE) {
        $identifier = substr($rowCsvIfa, 0, 2);
        $value = substr($rowCsvIfa, 2, -2);

        if ($identifier == '00') {
            $datacounter = 0;
            $nextdata = 0;

            if ($value == "I") {
                $nextdata = 1;

                $dataset = 1;
                $i++;
            }
            else {
               $break = 1;
            }

        }

        if($nextdata == 1 && $identifier != '00') {
            $dataarray[$i][$datacounter] = $value;

            $datacounter ++;
        }

        if($break == 1 && $dataset == 1) {
            $allarray[0] = $dataarray;

            return $allarray;
        }
    }
}

function getTablename($chIfa) {

    $nexttable = 0;
    //$nextfield = 0;
    //$nextdata = 0;
    //$nextupdate = 0;
    //$nextdelete = 0;

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

function createFieldArray($chIfa) {
    $fielarray = array();
    $insertcounter = 0;
    $fieldset = 0;
    $break = 0;
    $fieldindex = 0;

    //$nexttable = 0;
    $nextfield = 0;
    //$nextdata = 0;
    //$nextupdate = 0;
    //$nextdelete = 0;

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

function protoreplace($text) {
    $search  = array('\A22', '\a22', '\A23', '\a23', '\A24' , '\a24' , '\A25' , '\a25' , '\A29' , '\a29' , '\a33' , '\A45' , '\a45' , '\a63' , '\b63' , '\c22' , '\C27' , '\c27' , '\C49' , '\c49' , '\D27' , '\d27' , '\D63' , '\d63' , '\E22' , '\e22' , '\E23' , '\e23' , '\E24'  , '\e24'  , '\E25' , '\e25' , '\E27' , '\e27' , '\E33' , '\E66' , '\e63' , '\F63' , '\f63' , '\G32' , '\g32' , '\G63' , '\g63' , '\H63' , '\h63' , '\I22' , '\i22' , '\I23' , '\i23' , '\I24' , '\i24' , '\I25'  , '\i25' , '\I36'  , '\i36' , '\I45' , '\i45' , '\i63' , '\J63' , '\j63' , '\k63' , '\L63' , '\l63' , '\M33'  , '\m33' , '\m63' , '\N26' , '\n26' , '\N27' , '\n27' , '\n63' , '\O22' , '\o22' , '\O23' , '\o23' , '\O24' , '\o24' , '\O25' , '\o25' , '\O35' , '\o35' , '\O42' , '\o42' , '\O45' , '\o45' , '\o63' , '\P63' , '\p63' , '\Q63' , '\q63' , '\R27' , '\r27' , '\r63' , '\S27' , '\s27' , '\S39' , '\s39' , '\S49' , '\s49' , '\S63' , '\s63' , '\T27' , '\t27' , '\t63' , '\U22' , '\u22' , '\U23' , '\u23' , '\U24' , '\u24' , '\U25' , '\u25' , '\U29' , '\u29' , '\u63' , '\W63' , '\w63' , '\x63' , '\Y22' , '\y22' , '\Y25' , '\y25' , '\Y63' , '\y63' , '\Z27' , '\z27' , '\z63' , '\321' , '\323' , '\324' , '\325' , '\326' , '\327' , '\328' , '\329' , '\330' , '\333' , '\340' , '\341' , '\344' , '\345' , '\346' , '\347' , '\348' , '\351' , '\360' , '\361' , '\362' , '\363' , '\364' , '\365' , '\367' , '\372' , '\375' , '\380' , '\420' , '\421' , '\422' , '\423' , '\424' , '\425' , '\426' , '\427' , '\428' , '\429' , '\430' , '\431' , '\432' , '\435' , '\460' , '\462' , '\463' , '\465' , '\466' , '\467' , '\473' , '\520' , '\521' , '\535' , '\565' , '\900');
    $replace = array('Á', 'á', 'À', 'à', 'Â', 'â', 'Ä', 'ä', 'Å', 'å', '', 'Æ', 'æ', 'æ', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '');

    return str_replace($search, $replace, $text);
}


function createTable($tablename, $db_link, $fieldarray) {


    $sql = "CREATE TABLE IF NOT EXISTS ".$tablename." (";
    $i = 1;
    $u = 1;
    $primarykeys = array();
    $sql .= "UID int NOT NULL AUTO_INCREMENT , ";
    foreach ($fieldarray as $field) {

        //echo '<pre>';
        //echo var_dump($field);
        //echo '</pre>';
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

    echo '<pre>';
    //echo var_dump($primarykeys);
    echo $sql;
    echo '</pre>';



    if ($db_link->query($sql) === TRUE) {
        echo "Table created successfully";
    } else {
        echo "Error creating table: " . $db_link->error;
    }



}


function insertDataSingle($dataarray, $tablename, $db_link, $fieldarray) {


    $start = "INSERT INTO ".$tablename." (";

    $i = 1;
    foreach ($fieldarray as $field) {
        //echo '<pre>';
        //echo var_dump($field);
        //echo '</pre>';

        $start .= $field[1];
        if($i < count($fieldarray)) {
            $start .= ' , ';
        }
        $i++;
    }
    $start .= ') VALUES ';
    $d = 1;
    $u = 1;
    $select = $start;
    foreach ($dataarray as $datas) {
        $select = $start;
        $select .= '( ';
        //echo '<pre>';
        //echo var_dump($datas);
        //echo '</pre>';
        //$select .= $data;
        foreach ($datas as $data) {
            //echo '<pre>';
            //echo var_dump($data);
            //echo '</pre>';

            $select .= "'";
            //$select .= mysql_real_escape_string($data);
            //$firstkey = array_key_first($data);
            //$select .= $db_link->real_escape_string($data[$firstkey]);
            $select .= $db_link->real_escape_string($data);
            if($u < count($datas)) {
                $select .= "' ,";
            }
            else {
                $select .= "'";
            }
            $u++;
        }

        $select .= ') ';
        if($d < count($dataarray)) {
            //$select .= ' , ';
        }
        $u = 1;
        $d++;

        //echo '<pre>';
        //echo $select;
        //echo '</pre>';

        if ($db_link->query($select) === TRUE) {
            //echo "Import erfolgreich";
        } else {
            echo "Import nicht erfolgreich: " . $db_link->error.'</br>';
        }


    }
    //$select .= ')';






}
