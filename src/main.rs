/*************************************
Build Information:
rustc 1.19.0

General Idea of my Solution:

In order to meet the runtime and functionality
requirements for this database I set up a
transaction struct that contains two hashmaps.
The first hashmap contains the key-value pairs
which allows me to return a value to a user
given a key in O(1) time. The second hashmap
is for storing value counts. By keeping the
value counts within its own hashmap I am able
to return the number of variables that are set
to a given value in O(1) time as well.

Adding support for the BEGIN, COMMIT,
and ROLLBACK commands was done by setting up
two important variables. The first is a current
transaction variable which is the only transaction
variable that is directly mutated.
Secondly, I maintain a transaction stack.
This stack is only used if a BEGIN command
is found. Once BEGIN is run I add a copy
of the current transaction to the stack,
and then proceed with further commands.
This model makes it super easy to accept
nested transaction blocks and allow
for simple rollbacks and commits too.
**************************************/

#![allow(non_camel_case_types)]

use std::io;
use std::io::prelude::*;
use std::process;
use std::string::*;
use std::collections::HashMap;
use db_command::*;

enum db_command {
	SET(String, i32),
	GET(String),
	UNSET(String),
	NUMEQUALTO(i32),
	END,
	BEGIN,
	ROLLBACK,
	COMMIT
}

struct transaction {
    key_val: HashMap<String, i32>,    // holds key value pairs within transaction
    val_quant: HashMap<i32, i32>    // holds quantity of a given value
}

impl transaction {
	/* assciated method that returns a new transaction */
    fn new() -> transaction{
        transaction{
            key_val: HashMap::new(),
            val_quant: HashMap::new()
        }
    }

	/* performs actions required for the SET command */
	fn set(&mut self, key:String, val:i32) {
		if self.key_val.contains_key(key.as_str()) {
			if let Some(current_val) = self.key_val.get(key.as_str()) {
				/* decrement val_quant count for old value attached to key if key  exists */
				if self.val_quant.contains_key(&current_val) {
					if let Some(count) = self.val_quant.get_mut(&current_val) {
						*count -= 1;
					}
				}
			}
		}

		self.key_val.insert(key, val);
		/* increment the corresponding value within val_quant */
		if self.val_quant.contains_key(&val) {
			if let Some(count) = self.val_quant.get_mut(&val) {
				*count += 1;
			}
		}
		else {
			/* first insertion of value starts off the count in val_quant */
			self.val_quant.insert(val, 1);
		}
	}

	fn unset(&mut self, key:String) {
		/* if the given key doesn't exist then nothing needs to be done. */
		if self.key_val.contains_key(key.as_str()) {
			/* remove key from key_val table */
			let value_option = self.key_val.remove(key.as_str());

			if let Some(value) = value_option {
				/* decrement count within val_quant */
				if let Some(count) = self.val_quant.get_mut(&value) {
					*count -= 1;
				}
			}
		}
	}

	/* retrieves the value stored at a given key */
	fn get(&self, key:String) {
		if let Some(value) = self.key_val.get(key.as_str()) {
			println!("> {}", value);
		}
		else {
			println!("> NULL");
		}
	}

	fn num_equal_to(&self, key:i32) {
		if self.val_quant.contains_key(&key) {
			if let Some(count) = self.val_quant.get(&key) {
				println!("> {}", count)
			}
		}
		else {
			/* key does not exist within the current transaction */
			println!("> 0");
		}
	}
}

/* tests validity of given command. If valid returns a db_command. Otherwise, returns error message. */
fn is_valid_command(command: &Vec<&str>) -> Result<db_command, &'static str> {
	match command[0] {
		"SET" =>
			if command.len() == 3 {
				let key = String::from(command[1]);
				let value_container = command[2].parse::<i32>();
				if let Ok(value) = value_container {
					Ok(SET(key, value))
				}
				else {
					Err("> Invalid value of supplied to SET")
				}
			}
			else {
				Err("> Incorrect number of arguments for SET command")
			},

		"GET" =>
			if command.len() == 2 {
				let key = String::from(command[1]);
				Ok(GET(key))
			}
			else {
				Err("> Incorrect number of arguments for GET command")
			},
		"NUMEQUALTO" =>
			if command.len() == 2 {
				let value_container = command[1].parse::<i32>();
				if let Ok(value) = value_container {
					Ok(NUMEQUALTO(value))
				}
				else {
					Err("> Invalid value supplied to NUMEQUALTO")
				}
			}
			else {
				Err("> Incorrect number of arguments for NUMEQUALTO command")
			},
		"UNSET" =>
			if command.len() == 2 {
				let key = String::from(command[1]);
				Ok(UNSET(key))
			}
			else {
				Err("> Incorrect number of arguments for UNSET command")
			},
		"BEGIN" =>
		 	if command.len() == 1 {
				Ok(BEGIN)
			}
			else {
				Err("> Incorrect number of arguments for BEGIN command")
			},
		"ROLLBACK" =>
			if command.len() == 1 {
				Ok(ROLLBACK)
			}
			else {
				Err("> Incorrect number of arguments for ROLLBACK command")
			},
		"COMMIT" =>
			if command.len() == 1 {
				Ok(COMMIT)
			}
			else {
				Err("> Incorrect number of arguments for BEGIN command")
			},

		"END" =>
		if command.len() == 1 {
			Ok(END)
		}
		else {
			Err("> Incorrect number of arguments for END command")
		},

		_ => Err("> INVALID COMMAND")
	}
}

/* function that accepts a validated command and runs the command on the provided transaction */
fn dispatch_command(cmd:db_command, ct:&mut transaction, ts:&mut Vec<transaction>) {
    match cmd {
		SET(key, value) => ct.set(key, value),
		GET(key) => ct.get(key),
		NUMEQUALTO(value) => ct.num_equal_to(value),
		UNSET(key) => ct.unset(key),
		BEGIN => {
			/* Add current transaction to transaction stack */
			ts.push(transaction{
				val_quant: ct.val_quant.clone(),
				key_val: ct.key_val.clone()
			});
		},
		ROLLBACK => {
			if ts.len() == 0 {
				println!("> NO TRANSACTION");
			}
			else {
				if let Some(tran) = ts.pop() {
					ct.val_quant = tran.val_quant;
					ct.key_val = tran.key_val;
				}
			}
		},
		COMMIT => {
			if ts.len() == 0 {
				println!("> NO TRANSACTION");
			}
			else {
				ts.clear();
			}
		},
		END => process::exit(0)
	}
}

fn main() {
	let mut transaction_stack:Vec<transaction> = Vec::new();
	let mut current_transaction = transaction::new();
    /* read in database requests */
	let stdin = io::stdin();
	 for line in stdin.lock().lines() {
		let input = String::from(line.expect("Read Error"));
	    let command_and_args = input.split_whitespace()
		  							.collect::<Vec<&str>>();

		/* Generate db_command if valid and dispatch if no error found */
		let cmd = is_valid_command(&command_and_args);
		match cmd {
			Ok(db_cmd) => {
				println!("{}", input);	// this reprints the original command. Needed for HackerRank test cases.
				dispatch_command(db_cmd, &mut current_transaction, &mut transaction_stack);
			},
			Err(msg) => println!("{}", msg)
		}
	}
}
