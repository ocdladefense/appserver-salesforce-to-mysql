<?php


use Http\HttpResponse;
use Http\HttpHeader;
use Http\Http;
use Http\HttpRequest;
use Mysql\Database;




class SalesforceModule extends Module {

    private $mysqli;



    public function __construct() {

        parent::__construct();
    }



	function doQuery() {

		$request = $this->getRequest();
		$body = $request->getBody();

		
		$where = $body->where;
		$limit = $body->limit;
		$orderBy = $body->orderBy;
		$offset = $body->offset;
		$subquery = "";

		for($i = 0; $i < count($where); $i++) {
			$sub = $where[$i];
			if("areaOfInterest" == $sub->field) {
				unset($where[$i]);
				$format = " AND Id IN (SELECT Contact__c FROM AreaOfInterest__c WHERE Interest__c = '%s') ";
				$subquery = sprintf($format,$sub->value);
			}
		}

		$query = "SELECT Id, Name, FirstName, LastName, Ocdla_Member_Status__c, Ocdla_Occupation_Field_Type__c, Ocdla_Organization__c, Phone, Email, MailingAddress, Ocdla_Current_Member_Flag__c, Ocdla_Is_Expert_Witness__c FROM Contact ";


		$countq = "SELECT COUNT(Id) FROM Contact ";

		if($where != null)
		{
            //null position and limit bug fixed
            //unsure if we can persist feature on map and change its datasource
            //may need to update library, mapfeature class
			$query .= "WHERE MailingLatitude != null AND ";
			$countq .= "WHERE MailingLatitude != null AND ";

			$struct = [];
			foreach($where as $obj)
			{
                if (is_bool($obj->value))
                {
                    $struct[]= $obj->field." ".$obj->op." ".($obj->value ? "True" : "False");
                }
                else 
                {
				    $struct[]= $obj->field." ".$obj->op." '".$obj->value."'";
                }
			}
			$query .= implode(" AND ",$struct);
			$countq .= implode(" AND ",$struct);
		}

        $query .= $subquery;
		$countq .= $subquery;


		if($orderBy != null) {
			$query .= " ORDER BY ".$orderBy;
		}

        if($limit != null)
		{
			$query .= " LIMIT ".$limit;
		}

		if($offset != null)
		{
			$query .= " OFFSET ".$offset;
		}
        //var_dump($query);

		$api = loadApi();

		$result = $api->query($query);
		$count = $api->query($countq);
		
        $myResult = array();
        $myResult["query"] = $query;
        $myResult["records"] = $result->getRecords();	
		$myResult["count"] = $count->getRecords()[0]["expr0"];


		return $myResult;		
	}



	public function transfer($soql, $table, $fields) {


		$mysql = new Mysql();
		$this->mysqli = $mysql->getConnection();

		// asdfjkl
		$lock = "LOCK TABLES %s WRITE";

		// Connect to Salesforce.
		$api = loadApi();

		// Store how many updates take place after the transfer to MySQL.
		$affectedRows = 0;

		// MySQL schema needed to properly formate the MySQL query.
		$schema = null;

		// Set to non-false value when paging through Salesforce records.
		$next = false;

		// Output to display.
		$out = [];



		$lockQuery = sprintf($lock, $table);
		
		$schema = $this->getSchema($table);



		$this->mysqli->query($lockQuery);


        do {

			$out []= $next ? "Starting batch {$next}." : "Performing query $soql.";
			$resp = $next ? $api->queryUrl($next) : $api->queryIncludeDeleted($soql, true);

			if(!$resp->isSuccess()) throw new Exception($resp->getErrorMessage());


            $body = $resp->getBody();
			$totalSize = $body["totalSize"];
			$done = $body["done"];
			$records = $body["records"];
            $next = !$done ? $body["nextRecordsUrl"] : null;

			if(!count($records)) {
				return "No records founds for importing...";
			}
			
			
			// Convert objects returned from REST API to rows 
			// that can be inserted into MySQL database.
			$rows = $this->getRows($records, $fields, $schema);

			// Then use these rows to construct a properly-formatted INSERT query.
			$query = $this->getInsertQuery($table, $rows, $fields);
			
	

			$out []= $query;
			// print $query;
			// exit;

			$result = $this->mysqli->query($query);
			// var_dump($conn->affected_rows);
			// var_dump($result);

			$affectedRows += $this->mysqli->affected_rows;
		
        
        } while($next);
		

		$this->mysqli->query("UNLOCK TABLES");
		$out []= ("<br />" . $affectedRows ." rows were updated.");

		return implode("<br />", $out);
	}





	public function getInsertQuery($table, $rows, $fields) {

		// Format of Insert query to MySQL.
		$insert = "INSERT INTO %s (%s) VALUES %s AS new(%s) ON DUPLICATE KEY UPDATE %s";

		// Prepare the MySQL insert query.
		$updates = array_filter($fields, function($f) { return $f != "Id"; });
		$updates = array_map(function($f) { return "{$f} = new.{$f}";}, $updates);
		$fieldList = implode(", ", $fields);
		$updateList = implode(", ", $updates);


		$rows = array_map(function($row) { return "(" . implode(", ",$row) . ")";}, $rows);
		$rowList = implode(", ", $rows);
			
		return sprintf($insert, strtolower($table), $fieldList, $rowList, $fieldList, $updateList);
	}




	public function getRows($records, $fields, $schema) {

		// We will insert/update rows into the appropriate table.
		$rows = array();

		
		// Assign values to the MySQL insert query.
		for($count = 0; $count < count($records); $count++) {

			$record = $records[$count];
			$row = array();

			foreach($fields as $field) {
				$value = $record[$field];

				// Convert boolean values to either 0 or 1.
				if($schema[$field]["is_int"] && is_bool($value)) {
					$value = $value ? 1 : 0;
				}
				if($schema[$field]["is_date"]) {
					$value = explode("+",$value)[0];
					$value = empty($value) ? null : $value;
				}
				$row[$field] = $schema[$field]["is_int"] ? $value : $this->mysqli->real_escape_string($value);
			}
			
			// Format string and date/time values with single quotes. Numbers shouldn't get quotes.
			$row = array_map(function($v,$k) use($schema) { 

				if(null == $v) {
					return "NULL";
				}
				else return $schema[$k]["is_int"] ? $v : "'{$v}'";
			}, $row, array_keys($row));

			$rows []= $row;
		}

		// var_dump($rows);exit;
		
		return $rows;
	}







	/**
	 * Return an array of schema statements for a given table.
	 * Schema statements can let us format the query,
	 * for example by identifying string columns that should be wrapped
	 * in quotes.
	 */
	public function getSchema($table) {

		// Get field metadata.
		$describe = "DESCRIBE %s";


		$query = sprintf($describe, $table);
	

		$schema = [];


		$result = $this->mysqli->query($query);

		while($info = $result->fetch_assoc()) {
			$field = $info["Field"];
			$type = $info["Type"];


			$schema[$field] = array(
				"type" => $info["Type"],
				"is_int" => (strpos($type,"int") !== false),
				"is_date" => (strpos($type,"date") !== false)
			);
		}


		return $schema;
	}





    /**
     * basic - Id, FirstName, LastName
     * donor_info - New donation fields.
     * contact_general - A lot of fields that are transferred to FileMaker.
     */
	public function transferRecentSObjectRecords($sobject = "contact", $list = "basic") {

		

		$date = new DateTime();
		$date->modify('-7 day');
		$date = $date->format('Y-m-d');

        $key = strtolower(implode(".", [$sobject,$list]));

		$config = loadModuleConfig("salesforce", $key);


		
		$fields = $config["fields"];
		$fields []= "CreatedDate";
		$fields []= "LastModifiedDate";
		$fieldListq = implode(", ", $fields);

		// Format of Select query to Salesforce.
		$select = $config["select"];
		
		// Determine whether the query already has a WHERE
		$hasWhere = count(explode("WHERE",$select)) > 1;
		
		$select .= ($hasWhere ? " AND " : " WHERE ");
		$select .= " LastModifiedDate >= %sT00:00:00Z";

		// Prepare Salesforce SOQL query.
		$soql = sprintf($select, $fieldListq, ucwords($sobject), $date);

		// Table that records should be transferred to.
		$table = $config["table"];

		return $this->transfer($soql, $table, $fields);
	}	




	/**
	 * Transfer 
	 */
	public function transferAll($sobject = "contact", $list = "basic") {


        
        $key = strtolower(implode(".", [$sobject,$list]));

		$config = loadModuleConfig("salesforce", $key);

		$fields = $config["fields"];
		$fields []= "CreatedDate";
		$fields []= "LastModifiedDate";
		$fieldListq = implode(", ", $fields);

		// Format of Select query to Salesforce.
		$select = $config["select"];


		// Prepare Salesforce SOQL query.
		$soql = sprintf($select, $fieldListq, ucwords($sobject));

		// Table that records should be transferred to.
		$table = $config["table"];

		return $this->transfer($soql, $table, $fields);
	}	






}