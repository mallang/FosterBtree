use std::collections::HashMap;

use chrono::NaiveDateTime; // Add this at the top
use rand::distributions::Distribution;
use rand::{distributions::uniform::UniformSampler, rngs::SmallRng, Rng, SeedableRng};

use super::Cli;

const C_LAST_PARTS: [&'static str; 10] = [
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];

const VALUE_SIZE: usize = size_of::<Customer>();

#[derive(Debug, Clone)]
pub struct Customer {
    cId: i32,
    dId: i32,
    wId: i32,
    C_FIRST: [u8; 16],
    C_MIDDLE: [u8; 2],
    C_LAST: [u8; 16],
    C_STREET_1: [u8; 20],
    C_STREET_2: [u8; 20],
    C_CITY: [u8; 20],
    C_STATE: [u8; 2],
    C_ZIP: [u8; 9],
    C_PHONE: [u8; 16],
    C_SINCE: u64, // convert to u64 further processing
    C_CREDIT: [u8; 2],
    C_CREDIT_LIM: f64,
    C_DISCOUNT: f64,
    C_BALANCE: f64,
    C_YTD_PAYMENT: f64,
    C_PAYMENT_CNT: i32,
    C_DELIVERY_CNT: i32,
    C_DATA: [u8; 500],
    C_N_NATIONKEY: i32,
}

impl Customer {
    pub fn new() -> Self {
        Customer {
            cId: 0,
            dId: 0,
            wId: 0,
            C_FIRST: [b' '; 16],
            C_MIDDLE: [b' '; 2],
            C_LAST: [b' '; 16],
            C_STREET_1: [b' '; 20],
            C_STREET_2: [b' '; 20],
            C_CITY: [b' '; 20],
            C_STATE: [b' '; 2],
            C_ZIP: [b' '; 9],
            C_PHONE: [b' '; 16],
            C_SINCE: 0,
            C_CREDIT: [b' '; 2],
            C_CREDIT_LIM: 0.0,
            C_DISCOUNT: 0.0,
            C_BALANCE: 0.0,
            C_YTD_PAYMENT: 0.0,
            C_PAYMENT_CNT: 0,
            C_DELIVERY_CNT: 0,
            C_DATA: [b' '; 500],
            C_N_NATIONKEY: 0,
        }
    }

    pub fn generate_pkey(&self) -> [u8; 12] {
        let mut pkey = [0u8; 12];
        pkey[..4].copy_from_slice(&self.cId.to_le_bytes());
        pkey[4..8].copy_from_slice(&self.dId.to_le_bytes());
        pkey[8..12].copy_from_slice(&self.wId.to_le_bytes());
        pkey
    }

    pub fn generate_join_key(&self) -> [u8; 16] {
        let mut join_key = [0u8; 16];
        join_key[..16].copy_from_slice(&self.C_LAST);
        join_key
    }

    pub fn generate_value(&self) -> [u8; VALUE_SIZE] {
        let mut value = [0u8; VALUE_SIZE];
        let mut offset = 0;

        // Copying fields to value
        value[offset..offset + 4].copy_from_slice(&self.cId.to_le_bytes());
        offset += 4;
        value[offset..offset + 4].copy_from_slice(&self.dId.to_le_bytes());
        offset += 4;
        value[offset..offset + 4].copy_from_slice(&self.wId.to_le_bytes());
        offset += 4;

        value[offset..offset + 16].copy_from_slice(&self.C_FIRST);
        offset += 16;
        value[offset..offset + 2].copy_from_slice(&self.C_MIDDLE);
        offset += 2;
        value[offset..offset + 16].copy_from_slice(&self.C_LAST);
        offset += 16;

        value[offset..offset + 20].copy_from_slice(&self.C_STREET_1);
        offset += 20;
        value[offset..offset + 20].copy_from_slice(&self.C_STREET_2);
        offset += 20;
        value[offset..offset + 20].copy_from_slice(&self.C_CITY);
        offset += 20;

        value[offset..offset + 2].copy_from_slice(&self.C_STATE);
        offset += 2;
        value[offset..offset + 9].copy_from_slice(&self.C_ZIP);
        offset += 9;
        value[offset..offset + 16].copy_from_slice(&self.C_PHONE);
        offset += 16;

        value[offset..offset + 8].copy_from_slice(&self.C_SINCE.to_le_bytes());
        offset += 8;
        value[offset..offset + 2].copy_from_slice(&self.C_CREDIT);
        offset += 2;
        value[offset..offset + 8].copy_from_slice(&self.C_CREDIT_LIM.to_le_bytes());
        offset += 8;

        value[offset..offset + 8].copy_from_slice(&self.C_DISCOUNT.to_le_bytes());
        offset += 8;
        value[offset..offset + 8].copy_from_slice(&self.C_BALANCE.to_le_bytes());
        offset += 8;
        value[offset..offset + 8].copy_from_slice(&self.C_YTD_PAYMENT.to_le_bytes());
        offset += 8;

        value[offset..offset + 4].copy_from_slice(&self.C_PAYMENT_CNT.to_le_bytes());
        offset += 4;
        value[offset..offset + 4].copy_from_slice(&self.C_DELIVERY_CNT.to_le_bytes());
        offset += 4;
        value[offset..offset + 500].copy_from_slice(&self.C_DATA);
        offset += 500;
        value[offset..offset + 4].copy_from_slice(&self.C_N_NATIONKEY.to_le_bytes());
        offset += 4;

        // assert_eq!(offset + 1, VALUE_SIZE, "Value size mismatch: expected {}, got {}", VALUE_SIZE, offset);
        value
    }
}

pub struct TransactionalOp {
    pub pkey: Vec<u8>,
    pub join_key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct DataSource {
    cli: Cli,
    rng: SmallRng,
    customers: Vec<Customer>,
    map_customers: HashMap<(i32, i32, i32), Customer>,
    ops: Vec<TransactionalOp>,
}

impl DataSource {
    pub fn new(cli: Cli) -> Self {
        let mut rng = SmallRng::seed_from_u64(cli.seed);
        let customers = Vec::new();
        let map_customers = HashMap::new();
        let ops = Vec::new();

        DataSource {
            cli,
            rng,
            customers,
            map_customers,
            ops,
        }
    }

    fn get_current_time_string() -> String {
        // This function should return the current time as a string
        // For simplicity, we will return a placeholder string here
        "2023-10-01 12:00:00".to_string()
    }

    fn parse_time_to_u64(time_str: &str) -> u64 {
        let dt = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S")
            .expect("Invalid date format");
        dt.and_utc().timestamp() as u64
    }

    fn gen_alphanumeric64(&mut self, min_length: usize, max_length: usize) -> String {
        let mut s = String::new();
        let mut rand = 1_u8;
        let length = rand::distributions::uniform::UniformInt::<usize>::new_inclusive(
            min_length, max_length,
        )
        .sample(&mut self.rng);

        for i in 0..length {
            rand = 0;
            while rand == 0
                || (rand > '9' as u8 && rand < 63)
                || (rand > 'Z' as u8 && rand < 'a' as u8)
            {
                rand = rand::distributions::uniform::UniformInt::<u8>::new_inclusive(
                    '0' as u8, 'z' as u8,
                )
                .sample(&mut self.rng);
            }
            s.push(char::from(rand as u8));
        }

        s
    }

    fn gen_alphanumeric62(&mut self, min_length: usize, max_length: usize) -> String {
        let mut s = String::new();
        let mut rand = 1_u8;
        let length = rand::distributions::uniform::UniformInt::<usize>::new_inclusive(
            min_length, max_length,
        )
        .sample(&mut self.rng);

        for i in 0..length {
            rand = 0;
            while rand == 0
                || (rand > '9' as u8 && rand < 'A' as u8)
                || (rand > 'Z' as u8 && rand < 'a' as u8)
            {
                rand = rand::distributions::uniform::UniformInt::<u8>::new_inclusive(
                    '0' as u8, 'z' as u8,
                )
                .sample(&mut self.rng);
            }
            s.push(char::from(rand as u8));
        }

        s
    }

    fn gen_numeric(&mut self, min_length: usize, max_length: usize) -> String {
        let mut s = String::new();
        let length = rand::distributions::uniform::UniformInt::<usize>::new_inclusive(
            min_length, max_length,
        )
        .sample(&mut self.rng);

        for i in 0..length {
            let rand =
                rand::distributions::uniform::UniformInt::<u8>::new_inclusive('0' as u8, '9' as u8)
                    .sample(&mut self.rng);
            s.push(char::from(rand));
        }

        s
    }

    fn gen_double(&mut self, min: f64, max: f64, decimals: i32) -> f64 {
        let min = (min * 10f64.powi(decimals)) as i64;
        let max = (max * 10f64.powi(decimals)) as i64;
        let range = rand::distributions::uniform::Uniform::new(min, max);
        range.sample(&mut self.rng) as f64 / 10f64.powi(decimals)
    }

    fn gen_c_last(&mut self, mut value: i32) -> [u8; 16] {
        let mut c_last = [' ' as u8; 16];
        let bytes: &[u8] = C_LAST_PARTS[value as usize / 100].as_bytes();
        c_last[..bytes.len()].copy_from_slice(bytes);
        let mut offset = bytes.len();
        value %= 100;

        let bytes: &[u8] = C_LAST_PARTS[value as usize / 10].as_bytes();
        c_last[offset..offset + bytes.len()].copy_from_slice(bytes);
        offset += bytes.len();
        value %= 10;

        let bytes: &[u8] = C_LAST_PARTS[value as usize].as_bytes();
        c_last[offset..offset + bytes.len()].copy_from_slice(bytes);

        c_last
    }

    fn random_non_uniform_int(&mut self, A: i32, x: i32, y: i32, C: i32) -> i32 {
        let uni1 = rand::distributions::uniform::UniformInt::<i32>::new_inclusive(0, A)
            .sample(&mut self.rng);
        let uni2 = rand::distributions::uniform::UniformInt::<i32>::new_inclusive(x, y)
            .sample(&mut self.rng);
        ((uni1 | uni2) + C) % (y - x + 1) + x
    }

    fn random_c_last(&mut self) -> [u8; 16] {
        let value = self.random_non_uniform_int(255, 0, 999, 173);
        self.gen_c_last(value)
    }

    fn gen_customer(&mut self, c_id: i32, d_id: i32, w_id: i32, cur_time_str: &str) -> Customer {
        // This function should generate a customer string based on the provided parameters
        // For simplicity, we will return a placeholder string here
        let mut c = Customer::new();
        c.cId = c_id;
        c.dId = d_id;
        c.wId = w_id;
        {
            let mut fixed = [b' '; 16]; // 用空格填充整个数组
            let generated = self.gen_alphanumeric64(8, 16);
            let bytes = generated.as_bytes();
            fixed[..bytes.len()].copy_from_slice(bytes);
            c.C_FIRST = fixed;
        }

        c.C_MIDDLE = "OE".as_bytes().try_into().unwrap();

        {
            if c_id <= 1000 {
                c.C_LAST = self.gen_c_last(c_id - 1);
            } else {
                c.C_LAST = self.random_c_last();
            }
        }

        {
            let mut fixed = [b' '; 20];
            let generated = self.gen_alphanumeric64(10, 20);
            let bytes = generated.as_bytes();
            fixed[..bytes.len()].copy_from_slice(bytes);
            c.C_STREET_1 = fixed;
        }

        {
            let mut fixed = [b' '; 20];
            let generated = self.gen_alphanumeric64(10, 20);
            let bytes = generated.as_bytes();
            fixed[..bytes.len()].copy_from_slice(bytes);
            c.C_STREET_2 = fixed;
        }

        {
            let mut fixed = [b' '; 20];
            let generated = self.gen_alphanumeric64(10, 20);
            let bytes = generated.as_bytes();
            fixed[..bytes.len()].copy_from_slice(bytes);
            c.C_CITY = fixed;
        }

        let state = self.gen_alphanumeric62(2, 2);
        {
            c.C_STATE = state.as_bytes().try_into().unwrap();
        }
        {
            let zip_prefix = self.gen_numeric(4, 4);
            let full_zip = format!("{}{}", zip_prefix, "11111");
            c.C_ZIP = full_zip.as_bytes().try_into().unwrap();
        }
        c.C_PHONE = self.gen_numeric(16, 16).as_bytes().try_into().unwrap();
        c.C_SINCE = Self::parse_time_to_u64(cur_time_str);
        c.C_CREDIT = if self.rng.gen_bool(0.1) {
            "BC".as_bytes().try_into().unwrap()
        } else {
            "GC".as_bytes().try_into().unwrap()
        };
        c.C_CREDIT_LIM = 50000.0;
        c.C_DISCOUNT = self.gen_double(0.0, 0.5, 4);
        c.C_BALANCE = -10.0;
        c.C_YTD_PAYMENT = 10.0;
        c.C_PAYMENT_CNT = 1;
        c.C_DELIVERY_CNT = 0;
        {
            let mut fixed = [b' '; 500];
            let generated = self.gen_alphanumeric64(300, 500);
            let bytes = generated.as_bytes();
            fixed[..bytes.len()].copy_from_slice(bytes);
            c.C_DATA = fixed;
        }

        c.C_N_NATIONKEY = state.as_bytes()[0] as i32; // Placeholder for nation key, should be replaced with actual logic

        c
    }

    pub fn generate_customer_table(&mut self) {
        let current_time_string = Self::get_current_time_string();

        for wId in 1..=self.cli.warehouse_count as i32 {
            for dId in 1..=10 {
                for cId in 1..=3000 {
                    // Generate customer data for warehouse wId, district dId, customer cId
                    // This is a placeholder for the actual data generation logic
                    let customer = self.gen_customer(cId, dId, wId, &current_time_string);
                    self.customers.push(customer.clone());
                    self.map_customers.insert((cId, dId, wId), customer);
                }
            }
        }
    }

    pub fn get_custoemr_vec(&self) -> &Vec<Customer> {
        &self.customers
    }

    pub fn generate_transactional_op(&mut self) -> TransactionalOp {
        let hAmount = self.gen_double(1.0, 5000.0, 2);
        let x = rand::distributions::uniform::UniformInt::<i32>::new_inclusive(1, 100)
            .sample(&mut self.rng);

        let cId = self.random_non_uniform_int(1023, 1, 3000, 867);
        let dId = rand::distributions::uniform::UniformInt::<i32>::new_inclusive(1, 10)
            .sample(&mut self.rng);
        let wId = rand::distributions::uniform::UniformInt::<i32>::new_inclusive(
            1,
            self.cli.warehouse_count as i32,
        )
        .sample(&mut self.rng);

        let (cDid, cWid) = if x <= 85 {
            (dId, wId)
        } else {
            let c_did = rand::distributions::uniform::UniformInt::<i32>::new_inclusive(1, 10)
                .sample(&mut self.rng);
            if self.cli.warehouse_count == 1 {
                (c_did, wId)
            } else {
                let mut c_wid = rand::distributions::uniform::UniformInt::<i32>::new_inclusive(
                    1,
                    self.cli.warehouse_count as i32,
                )
                .sample(&mut self.rng);
                while c_wid == wId {
                    c_wid = rand::distributions::uniform::UniformInt::<i32>::new_inclusive(
                        1,
                        self.cli.warehouse_count as i32,
                    )
                    .sample(&mut self.rng);
                }
                (c_did, c_wid)
            }
        };

        let old_c = self.map_customers.get(&(cId, cDid, cWid)).unwrap();
        let mut new_c = old_c.clone();
        new_c.C_BALANCE -= hAmount;
        new_c.C_YTD_PAYMENT += hAmount;
        new_c.C_PAYMENT_CNT += 1;

        self.map_customers.insert((cId, cDid, cWid), new_c.clone());
        TransactionalOp {
            pkey: new_c.generate_pkey().to_vec(),
            join_key: new_c.generate_join_key().to_vec(),
            value: new_c.generate_value().to_vec(),
        }
    }

    pub fn generate_join_key(&mut self) -> [u8; 16] {
        self.random_c_last()
    }
}
