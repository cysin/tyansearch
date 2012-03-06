<?php
//echo '[ true, false, "\u20AC" ]';
//exit();
mysql_connect('127.0.0.1', 'root', 'root');
mysql_select_db('news');
mysql_query("SET NAMES utf8");

$sql = "SELECT * FROM articles ORDER BY id ASC LIMIT 5";
$query = mysql_unbuffered_query($sql);

$i=0;
echo "[";
while($row = mysql_fetch_assoc($query)) {
	empty($record);
	$record['id'] = $row['id'];
	$record['category'] = $row['category'];
	$record['title'] = $row['title'];
	$record['content'] = $row['content'];
	echo json_encode($record);
	echo ",";
}
echo "]";


