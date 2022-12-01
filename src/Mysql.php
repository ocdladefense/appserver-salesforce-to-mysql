<?php
use Mysql\Database;


class Mysql {


    private $mysqli;


    public function __construct() {

		$config = loadModuleConfig("salesforce");
		$params = array(
			"host" => $config["salesforce.transfer.host"],
			"user" => $config["salesforce.transfer.user"],
			"password" => $config["salesforce.transfer.password"],
			"name" => $config["salesforce.transfer.database"]
		);


		Database::setDefault($params);
		$db = new Database();
		$mysqli = $db->getConnection();

		$mysqli->set_charset("utf8mb4");
		$mysqli->query("SET NAMES utf8mb4 COLLATE utf8mb4_0900_as_cs");
        $this->mysqli = $mysqli;
    }

    public function getConnection() {

        return $this->mysqli;
    }
}