PhonePe Pulse - Data
The Indian digital payments story has truly captured the world’s imagination. From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet and state-of-art payments infrastructure built as Public Goods championed by the central bank and the government. PhonePe started in 2016 and has been a strong beneficiary of the API driven digitisation of payments in India. When we started , we were constantly looking for definitive data sources on digital payments in India without much success. As a way of giving back to the data and developer community, we decided to open the anonymised aggregate data sets that demystify the what, why and how of digital payments in India. Licensed under the CDLA-Permissive-2.0 open data license, the PhonePe Pulse Dataset API is a first of its kind open data initiative in the payments space.

Announcements
🌟 Data for Q3(July, August, September) and Q4(Octobar, November, December) of 2024 has been added and is available for consumption.

Table of Contents
PhonePe Pulse - Data
Announcements
Table of Contents
Goal
Guide
Documentation
Folder Structure
JSON Structure / Syntax
Aggregated
data/aggregated/transaction/country/india/2018/1.json
data/aggregated/user/country/india/2021/1.json
data/aggregated/insurance/country/india/2021/1.json
Map
data/map/transaction/hover/country/india/2021/1.json
data/map/user/hover/country/india/2021/1.json
data/map/insurance/hover/country/india/2021/1.json
Top
data/top/transaction/country/india/2021/1.json
data/top/user/country/india/2021/1.json
data/top/insurance/country/india/2021/1.json
FAQs
LICENSE
Goal
Our goal is to share this data with everyone (license below), so that you can build your own understanding, insights and visualization on how digital payments have evolved over the years in India.

Guide
This data has been structured to provide details of following three sections with data cuts on Transactions, Users and Insurance of PhonePe Pulse - Explore tab.

Aggregated - Aggregated values of various payment categories as shown under Categories section
Map - Total values at the State and District levels.
Top - Totals of top States / Districts /Pin Codes
All the data provided in these folders is of JSON format. For more details on the structure/syntax you can refer to the JSON Structure / Syntax section of the documentation.

Documentation
Folder Structure
Head to the data folder to the find below shown structure. Overall, above mentioned sections data can be found at top level folder structure.

Under each of these sections there are folders for Transactions, Users and Insurance respectively.

Data for Transactions, Users and Insurance is grouped under country level within India folder which further grouped the data into each year(for country level data) and there is one folder with name state which groups data for all the available states of India respectively.

Similar to country level data, state level data too grouped into each year. All of these year folders(both at country and state level) have a maximum of four files with names starting from 1 to 4. These numbers represent each quarter in the selected year.

Example: 2021 > 1.json represents data for quarter 1 (Jan, Feb and Mar 2021)

For details on syntax of each of these files, refer to JSON Structure / Syntax.

data
|___ aggregated
    |___ transactions
        |___ country
            |___ india
                |___ 2018
                |    1.json
                |    2.json
                |    3.json
                |    4.json

                |___ 2019
                |    ...
                |___ 2019
                |___ state
                    |___ andaman-&-nicobar-islands
                        |___2018
                        |   1.json
                        |   2.json
                        |   3.json
                        |   4.json

                    |___ andhra-pradesh
                    |    ...
                    |    ...
JSON Structure / Syntax
1. Aggregated
1.1 data/aggregated/transaction/country/india/2018/1.json
Transaction data broken down by type of payment at country level.

For complete details on syntax find the comments in below code

NOTE: Similar syntax is followed for state level too. Ex: data/aggregated/transaction/country/india/state/delhi/2018/1.json

{
    "success": true, //Ignore. For internal use only
    "code": "SUCCESS", //Ignore. For internal use only
    "data": {
        "from": 1514745000000, //Data duration
        "to": 1522175400000,
        "transactionData": [
            {
                "name": "Recharge & bill payments", //Type of payment category
                "paymentInstruments": [
                    {
                        "type": "TOTAL",
                        "count": 72550406, //Total number of transactions for the above payment category
                        "amount": 1.4472713558652578E10 //Total value
                    }
                ]
            },

            ...,

            ...,

            {
                "name": "Others",
                "paymentInstruments": [
                    {
                        "type": "TOTAL",
                        "count": 5761576,
                        "amount": 4.643217301269438E9
                    }
                ]
            }
        ]
    },
    "responseTimestamp": 1630346628866 //Ignore. For internal use only.
}
1.2 data/aggregated/user/country/india/2021/1.json
Users data broken down by devices at country level.

For complete details on syntax find the comments in below code

NOTE: Similar syntax is followed for state level too. Ex: data/aggregated/user/country/india/state/delhi/2021/1.json

{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "aggregated": {
            "registeredUsers": 284985430, //Total number of registered users for the selected quarter.
            "appOpens": 8635508502 //Number of app opens by users for the selected quarter
        },
        "usersByDevice": [ //Users by individual device
            {
                "brand": "Xiaomi", //Brand name of the device
                "count": 71553154, //Number of registered users by this brand.
                "percentage": 0.2510765339828075 //Percentage of share of current device type compared to all devices.
            },

            ...,

            ...,

            {
                "brand": "Others", //All unrecognized device types grouped here.
                "count": 23564639, //Number of registered users by all unrecognized device types.
                "percentage": 0.08268717105993804 //Percentage of share of all unrecognized device types compared to overall devices that users are registered with.
            }
        ]
    },
    "responseTimestamp": 1630346630074 //Ignore. For internal use only.
}
1.3 data/aggregated/insurance/country/india/2021/1.json
Insurance data at country level.

For complete details on syntax find the comments in below code

NOTE: Similar syntax is followed for state level too. Ex: data/aggregated/insurance/country/india/state/delhi/2021/1.json

{
    "success": true, //Ignore. For internal use only
    "code": "SUCCESS", //Ignore. For internal use only
    "data": {
        "from": 1609439400000, //Data duration
        "to": 1616869800000,
        "transactionData": [
            {
                "name": "Insurance", //Type of payment category
                "paymentInstruments": [
                    {
                        "type": "TOTAL",
                        "count": 318119, //Total number of insurance done for the above duration
                        "amount": 1.206307024 //Total value
                    }
                ]
            },
        ]
    },
    "responseTimestamp": 1630346628866 //Ignore. For internal use only.
}
2. Map
2.1 data/map/transaction/hover/country/india/2021/1.json
Total number of transactions and total value of all transactions at the state level.

For complete details on syntax find the comments in below code

NOTE: Similar syntax is followed for district level too. Ex: data/map/transaction/hover/country/india/state/delhi/2021/1.json

{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "hoverDataList": [ //Internally, this being used to show state/district level data whenever a user hovers on a particular state/district.
            {
                "name": "puducherry", //State / district name
                "metric": [
                    {
                        "type": "TOTAL",
                        "count": 3309432, //Total number of transactions done within the selected year-quarter for the current state/district.
                        "amount": 5.899309571743641E9 //Total transaction value within the selected year-quarter for the current state/district.
                    }
                ]
            },

            ...,

            ...,

            {
                "name": "tamil nadu",
                "metric": [
                    {
                        "type": "TOTAL",
                        "count": 136556674,
                        "amount": 2.4866814387365314E11
                    }
                ]
            }
        ]
    },
    "responseTimestamp": 1630346628834 //Ignore. For internal use only.
}
2.2 data/map/user/hover/country/india/2021/1.json
Total number of registered users and number of app opens by these registered users at the state level.

For complete details on syntax find the comments in below code

NOTE: Similar syntax is followed for district level too. Ex: data/map/user/hover/country/india/state/delhi/2021/1.json

{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "hoverData": { //Internally, this being used to show state/district level data whenever a user hovers on a particular state/district.
            "puducherry": {
                "registeredUsers": 346279, //Total number of registered users for the selected state/district
                "appOpens": 7914507 //Total number of app opens by the registered users for the selected state/district
            },

            ...,

            ...,

            "tamil nadu": {
                "registeredUsers": 16632608,
                "appOpens": 348801714
            }
        }
    },
    "responseTimestamp": 1630346628866 //Ignore. For internal use only.
}
2.3 data/map/insurance/hover/country/india/2021/1.json
Total number of insurance and total value of all insurance at the state level.

For complete details on syntax find the comments in below code

NOTE: Similar syntax is followed for district level too. Ex: data/map/insurance/hover/country/india/state/delhi/2021/1.json

{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "hoverDataList": [ //Internally, this being used to show state/district level data whenever a user hovers on a particular state/district.
            {
                "name": "puducherry", //State / district name
                "metric": [
                    {
                        "type": "TOTAL",
                        "count": 3309432, //Total number of insurance done within the selected year-quarter for the current state/district.
                        "amount": 5.899309571743641E9 //Total insurance value within the selected year-quarter for the current state/district.
                    }
                ]
            },

            ...,

            ...,

            {
                "name": "tamil nadu",
                "metric": [
                    {
                        "type": "TOTAL",
                        "count": 136556674,
                        "amount": 2.4866814387365314E11
                    }
                ]
            }
        ]
    },
    "responseTimestamp": 1630346628834 //Ignore. For internal use only.
}
3. Top
3.1 data/top/transaction/country/india/2021/1.json
Top 10 states / districts / pin codes where the most number of the transactions happened for a selected year-quarter combination.

For complete details on syntax find the comments in below code

NOTE: Similar syntax is followed for state level too. The only exception is, it won't have data for states. Ex: data/top/transaction/country/india/state/delhi/2021/1.json

{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "states": [ //List of states where most number of transactions happened along with total value for a selected year-quarter combination.
            {
                "entityName": "karnataka", // State name
                "metric": {
                    "type": "TOTAL",
                    "count": 523797492, //Total number of transactions
                    "amount": 7.549953574123948E11 //Total value of all transactions
                }
            },

            ...,
        ],
        "districts": [ //List of districts where most number of transactions happened along with total value for a selected year-quarter combination.
            {
                "entityName": "bengaluru urban", //District name
                "metric": {
                    "type": "TOTAL",
                    "count": 348712787, //Total number of transactions
                    "amount": 4.324013412317671E11 //Total value of all transactions
                }
            },

            ...,
        ],
        "pincodes": [ //List of pin codes where most number of transactions happened along with total value for a selected year-quarter combination.
            {
                "entityName": "560001", //Pin code
                "metric": {
                    "type": "TOTAL",
                    "count": 111898471, //Total number of transactions
                    "amount": 1.5427512629157785E11 //Total value of all transactions
                }
            },

            ...,
        ]
    },
    "responseTimestamp": 1630346629360 //Ignore. For internal use only.
}
3.2 data/top/user/country/india/2021/1.json
Top 10 states / districts / pin codes where most number of users registered from, for a selected year-quarter combination.

For complete details on syntax find the comments in below code

NOTE: Similar syntax is followed for state level too. The only exception is, it won't have data for states. Ex: data/top/user/country/india/state/delhi/2021/1.json

{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "states": [ //List of states where the most number of users registered from, for a selected year-quarter combination.
            {
                "name": "maharashtra", //State name
                "registeredUsers": 37077537 //Number of registered users
            },

            ...,
        ],
        "districts": [ //List of districts where the most number of users registered from, for a selected year-quarter combination.
            {
                "name": "bengaluru urban", //State name
                "registeredUsers": 9955387 //Number of registered users
            },

            ...,
        ],
        "pincodes": [ //List of pin codes where most number of users registered from, for a selected year-quarter combination.
            {
                "name": "201301", //Pin code
                "registeredUsers": 541127 //Number of registered users
            },

            ...,
        ]
    },
    "responseTimestamp": 1630346630074 //Ignore. For internal use only.
}
3.3 data/top/insurance/country/india/2021/1.json
Top 10 states / districts / pin codes where the most number of the insurance happened for a selected year-quarter combination.

For complete details on syntax find the comments in below code

NOTE: Similar syntax is followed for state level too. The only exception is, it won't have data for states. Ex: data/top/insurance/country/india/state/delhi/2021/1.json

{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "states": [ //List of states where most number of insurance happened along with total value for a selected year-quarter combination.
            {
                "entityName": "karnataka", // State name
                "metric": {
                    "type": "TOTAL",
                    "count": 523797492, //Total number of insurance
                    "amount": 7.549953574123948E11 //Total value of all insurance
                }
            },

            ...,
        ],
        "districts": [ //List of districts where most number of insurance happened along with total value for a selected year-quarter combination.
            {
                "entityName": "bengaluru urban", //District name
                "metric": {
                    "type": "TOTAL",
                    "count": 348712787, //Total number of insurance
                    "amount": 4.324013412317671E11 //Total value of all insurance
                }
            },

            ...,
        ],
        "pincodes": [ //List of pin codes where most number of insurance happened along with total value for a selected year-quarter combination.
            {
                "entityName": "560001", //Pin code
                "metric": {
                    "type": "TOTAL",
                    "count": 111898471, //Total number of insurance
                    "amount": 1.5427512629157785E11 //Total value of all insurance
                }
            },

            ...,
        ]
    },
    "responseTimestamp": 1630346629360 //Ignore. For internal use only.
}
FAQs
How frequently data gets updated?
A: The frequency is once in three months.
How to report a bug or raise a query?
A: Use GitHub issues to raise a query.
LICENSE
Community Data License Agreement – Permissive – Version 2.0
