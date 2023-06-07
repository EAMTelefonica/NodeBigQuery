import {BigQuery} from '@google-cloud/bigquery';
import csv from 'fast-csv'
import { json2csv } from 'json-2-csv';
import {execa, execaCommand} from 'execa';
import fs from 'fs'
import 'dotenv/config';
import winston from 'winston';

const dateforLogs=new Date().toISOString().replace('-', '_').substr(0, 10);
const { combine, timestamp, json } = winston.format;
const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: combine(timestamp(), json()),
    transports: [
      new winston.transports.Console(),
      new winston.transports.File({ filename: `Logs/${dateforLogs}combined.log` }) ],
  });


const pathtoKey=process.env.PATH_TO_KEY;
const DbPass=process.env.DB_PASS;
const DbServer=process.env.DB_SERVER;
const DbPort=process.env.DB_PORT;
const projID=process.env.PROJECT_ID;

const options = {
    keyFilename: pathtoKey,
    projectId: projID,
  };


const bigquery = new BigQuery(options);




let results=[]
async function query() {
    // Queries the U.S. given names dataset for the state of Texas.

    const query = `SELECT DISTINCT country_id, channel_id, api_id
      FROM \`bigquery-global-grn\`.DATOS_COMPARTIDOS_PLATAFORMAS_GLOBALES.api_kpis_daily_metrics_channel`;

    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    const options = {
      query: query
      // Location must match that of the dataset(s) referenced in the query.
      
    };

    // Run the query as a job
    const [job] = await bigquery.createQueryJob(options);
    console.log(`Job ${job.id} started.`);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();
    results =rows
    // Print the results
    // console.log('Rows:');
    // rows.forEach(
    //     row => console.log(row)
    //     );
        console.log(results)
        await createCSVFromJsonAndSave(results)
        await loadFilesToDb();
  }

async function createCSVFromJsonAndSave(json){
    let resultado= await json2csv(json)
    fs.writeFileSync('Results/csvtoimport.csv', resultado)
}


async function loadFilesToDb(){
    try {
        const csvsql=`csvsql --db postgresql://postgres:${DbPass}@${DbServer}:${DbPort}/baikalglobal --tables hermesKPI --insert --overwrite --create-if-not-exists Results/csvtoimport.csv`
        await execaCommand(csvsql);
    } catch (error) {
      logger.error(`Error happened, ${error}` );
      console.log(error);
    }
}

  query();
  
