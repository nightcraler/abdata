<?php

// Datenbank IFA-Preise (für IFA importer.php + index.php)
/*
$mysql_hoster = 'db001113.mydbserver.com';
$mysql_nutzer = 'p580045';
$mysql_passwort = 'Oracle01!';
$mysql_datenbank = 'usr_p580045_26';
*/
$mysql_hoster = 'localhost';
$mysql_nutzer = 'root';
$mysql_passwort = 'root';
$mysql_datenbank = 'abdata';

// Ordner mit zu verarbeitenden/verarbeiteten ILD-Dateien, organisiert in Unterordnern mit Datum (z.B. data/YYMMDD/...ild)
define("BASE_FOLDER", "./daten");
define("DONE_FOLDER", "./done");

$GLOBALS['dblink'] = mysqli_connect($mysql_hoster, $mysql_nutzer, $mysql_passwort);
mysqli_set_charset($GLOBALS['dblink'], 'utf8');
mysqli_select_db($GLOBALS['dblink'], $mysql_datenbank);
