using System;
using System.IO;
using System.Net;
using MySql.Data.MySqlClient;
using System.Collections.Generic;

namespace STSLabTestGuarnieri
{
    // Struct che rappresenta le righe da appendere al file di testo FTP nominato "job"
    public struct StringToAppend
    {
        public string idRecord;
        public string codeOfOrder;
        public string idProduct;
        public string nameProduct;
        public string numberOfPieces;

        public StringToAppend(string idRecord, string codeOfOrder, string idProduct, string nameProduct, string numberOfPieces)
        {
            this.idRecord = idRecord;
            this.codeOfOrder = codeOfOrder;
            this.idProduct = idProduct;
            this.nameProduct = nameProduct;
            this.numberOfPieces = numberOfPieces;
        }
    }

    // Struct che rappresentas un id prodotto con il suo nome specifico
    public struct idNamesProducts
    {
        public string id;
        public string name;

        public idNamesProducts(string id, string name)
        {
            this.id = id;
            this.name = name;
        }
    }

    class Program
    {
        /************* MYSQL DB VARIABLE *************/
        // Indirizzo del Server
        private string datasource = "79.10.111.184";
        // Nome del database
        private string database = "sck_dev";
        // Username
        private string username = "stslab_test";
        // Password
        private string password = "zy'@7FP#";
        // Numero porta
        private string portNumber = "2002";
        // Variabile che rappresenta la connessione con il db
        private MySqlConnection conn;
        // Variabile che rappresenta i comandi da inviare come query SQL al db
        MySqlCommand command;
        /*********************************************/

        // Lista che conterrà le stringhe da scrivere nel file
        List<StringToAppend> StringToAddList = new List<StringToAppend>();
        // Lista che conterrà gli id e i nomi dei prodotti
        List<idNamesProducts> idNamesProducts = new List<idNamesProducts>();

        // Funzione per stampare testo sulla console con una determinata forma (datetime: text)
        public void printTextToConsole(string text)
        {
            DateTime now = DateTime.Now;
            Console.WriteLine(now + ": " + text);
        }

        // Funzione per aprire la connessione con il db
        public int openConnection()
        {
            // Stringa di connessione al db
            string connString = "Database=" + database + ";Port=" + portNumber + ";Data Source=" + datasource + ";User Id=" + username + ";Password=" + password + ";SslMode=none;";

            // Creo un'instanza della connessione con la stringa appena creata
            conn = new MySqlConnection(connString);

            // Avviso l'utente tramite il terminale
            printTextToConsole("Invio il comando di connessione al database");

            try
            {
                //Apro la connessione con il db
                conn.Open();
                // Avviso l'utente tramite il terminale
                printTextToConsole("Connessione con il db aperta con successo");
                // Ritorno codice positivo
                return 1;
            }
            catch (Exception e)
            {
                // Avviso l'utente tramite il terminale
                printTextToConsole("Errore nel tentativo di connessione con il db -> " + e.Message);
                // Ritorno codice negativo
                return -1;
            }
        }

        // Funzione per chiudere la connessione con il db
        public int closeConnection()
        {
            try
            {
                // Chiudo la connessione
                conn.Close();
                // Avviso l'utente tramite il terminale
                printTextToConsole("Connessione con il db chiusa");
                // Ritorno codice positivo
                return 1;
            }
            catch (Exception e)
            {
                // Avviso l'utente tramite il terminale
                printTextToConsole("Errore nel tentativo di chiusura di connessione con il db -> " + e.Message);
                // Ritorno codice negativo
                return -1;
            }
        }

        // Funzione per ritornare tutti i nomi dei prodotti relativi data una lista di id
        public void returnNameOfProduct(List<String> id)
        {
            try
            {
                // Query per ritornare il codce della commessa e i relativi prodotti e quantità
                var query = "SELECT nome_prodotto FROM prodotti WHERE id IN (";

                // Ritorno tutti gli id nella lista passata come argomento
                for (int i = 0; i < id.Count; i++)
                {
                    query += id[i];
                    if (i < id.Count - 1) query += ",";
                }
                query += ");";

                // Imposto la query al comando
                command = new MySqlCommand(query, conn);

                int j = 0;
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        idNamesProducts newItem = idNamesProducts[j];
                        newItem.name = reader.GetString("nome_prodotto");
                        idNamesProducts[j] = newItem;
                        j++;
                    }
                }

                return;
            }
            catch (Exception e)
            {
                // Avviso l'utente tramite il terminale
                printTextToConsole("Errore -> " + e.Message);
                // Esco dalla funzione
                return;
            }
        }

        public int returnOrders()
        {
            try
            {
                StringToAddList = new List<StringToAppend>();

                // Query per ritornare il codice della commessa e i relativi prodotti e quantità
                var query = "SELECT id, codice_commessa, status, products_data FROM sck_dev.commesse WHERE status IN (0,3);";
                // Imposto la query al comando
                command = new MySqlCommand(query, conn);

                // Avviso l'utente tramite il terminale
                printTextToConsole("Scarico dal database le commesse con status 0/3");

                // Eseguo la query e mi salvo i dati
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        // Mi salvo l'id del record, il codice della commessa e i vari prodotti:quantità
                        string idRecord = reader.GetString("id");
                        string orderNumber = reader.GetString("codice_commessa");
                        string idProducts = reader.GetString("products_data");

                        // Se questi non sono nulli o testo vuoto recupero l'id del prodotto e la sua quantità dalla colonna product_data
                        if (idProducts != "" && idProducts != " ")
                        {
                            string[] subs = idProducts.Split(',');

                            foreach (var sub in subs)
                            {
                                string[] id = sub.Split(':');

                                // Aggiungo alla lista la commessa trovata (un elemento della lista per ogni coppia prodotto:quantità)
                                StringToAddList.Add(new StringToAppend(idRecord, orderNumber, id[0], "", id[1]));
                            }
                        }

                    }
                }

                // ******** Cerco tutti i nomi degli id prodotti trovati precedentemente ******** //
                // Creo una lista che conterrà gli id univoci dei prodotti trovati
                List<String> idList = new List<string>();

                // Con un cilco for aggiungo gli id senza ripeterli
                for (int i = 0; i < StringToAddList.Count; i++)
                {
                    if (!idList.Contains(StringToAddList[i].idProduct))
                    {
                        idList.Add(StringToAddList[i].idProduct);
                        idNamesProducts.Add(new idNamesProducts(StringToAddList[i].idProduct, ""));
                    }
                }

                // Chiamo la funzione che cerca i nomi degli id prodotti e li aggiunge agli elementi specifici della lista idNamesProducts
                returnNameOfProduct(idList);

                // Aggiungo i nomi dei prodotti alla lista principale
                for (int i = 0; i < StringToAddList.Count; i++)
                {
                    idNamesProducts item = idNamesProducts.Find(item => item.id == StringToAddList[i].idProduct);

                    StringToAppend newItem = StringToAddList[i];
                    newItem.nameProduct = item.name;
                    StringToAddList[i] = newItem;

                }
                // ****************************************************************************** //

                // Ritorno un codice positivo
                return 1;

            }
            catch (Exception e)
            {
                // Avviso l'utente tramite il terminale
                printTextToConsole("Errore -> " + e.Message);
                // Ritorno un codice negativo
                return -1;
            }
        }

        public void insertRecordInImTopicsTable(string idOrder, string idProduct, string nPieces)
        {
            // Avviso l'utente tramite il terminale
            printTextToConsole("Inserisco la commessa modificata nel db");
            // Query per ritornare il codice della commessa e i relativi prodotti e quantità
            var query = "INSERT INTO im_topics(id_commessa, id_prodotto, message) VALUES('" + idOrder + "', '" + idProduct + "', '" + nPieces + "');";
            // Imposto la query al comando
            command = new MySqlCommand(query, conn);
            // Eseguo la query
            MySqlDataReader reader = command.ExecuteReader();
        }

        public void updateRowOfLocalJobFile(int idrow, string text)
        {
            try
            {
                // Imposto il path del file da modificare
                string localfilePath = Path.Combine(Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "STSLabTestGuarnieri/jobCopy.txt");
                string[] arrLine = File.ReadAllLines(localfilePath);
                arrLine[idrow] = text;
                File.WriteAllLines(localfilePath, arrLine);
                printTextToConsole("Commessa modificata nel file locale jobCopy.txt");
            }
            catch (Exception ex)
            {
                printTextToConsole("Errore durate la modifica di una commessa all'interno del file locale jobCopy.txt ->" + ex.Message);
            }

        }

        public void updateFTPJobFileFromLocalJobFile()
        {
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create("ftp://79.10.111.184/disk3/job.txt");
            request.Method = WebRequestMethods.Ftp.UploadFile;

            // This example assumes the FTP site uses anonymous logon.
            request.Credentials = new NetworkCredential("stslab_test3", "zy'@7FP#");

            // Copy the contents of the file to the request stream.
            string localfilePath = Path.Combine(Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "STSLabTestGuarnieri/jobCopy.txt");

            StreamReader file = new System.IO.StreamReader(localfilePath);
            string line = "", text = "";
            int count = 0;

            while ((line = file.ReadLine()) != null)
            {
                string[] temp = line.ToString().Split(",");
                for (int i = 0; i < 8; i++)
                {
                    text += temp[i];
                    if (i < 7) text += ",";
                }
                text += "\n";
                count++;
            }

            byte[] fileContents = new byte[count];
            fileContents = System.Text.Encoding.UTF8.GetBytes(text);
            file.Close();

            /*using (StreamReader sourceStream = new StreamReader(localfilePath))
            {
                fileContents = System.Text.Encoding.UTF8.GetBytes(sourceStream.ReadToEnd());
            }*/

            request.ContentLength = fileContents.Length;

            using (Stream requestStream = request.GetRequestStream())
            {
                requestStream.Write(fileContents, 0, fileContents.Length);
            }

            using (FtpWebResponse response = (FtpWebResponse)request.GetResponse())
            {
                Console.WriteLine($"Upload File Complete, status {response.StatusDescription}");
            }
        }

        public void checkJobCopyTextFile()
        {
            printTextToConsole("Controllo le commesse scaricate (inserimento o modifica)");

            // Copio il contenuto di jobCopy.txt (file locale copia del file in FTP) in una lista così da poter maneggiare meglio il contenuto
            string localfilePath = Path.Combine(Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "STSLabTestGuarnieri/jobCopy.txt");
            string[] text = System.IO.File.ReadAllLines(localfilePath);

            List<string> jobCopyList = new List<string>();

            for (int i = 0; i < text.Length; i++)
            {
                jobCopyList.Add(text[i]);
            }

            // Confronto le commesse trovate con il file in locale, se nel db ne sono state aggiunte di nuove queste vengono salvate in una stringa e successivamente aggiunte al file in locale e FTP
            string appendTextLocal = "";
            string appendTextFTP = "";
            for (int i = 0; i < StringToAddList.Count; i++)
            {
                var index = jobCopyList.Find(x => x.Contains(StringToAddList[i].idRecord + "," + StringToAddList[i].idProduct));
                if (index == null)
                {
                    Random rand = new Random();
                    appendTextLocal += "\r\n\"" + StringToAddList[i].codeOfOrder + "\",\"" + StringToAddList[i].nameProduct + "\"," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + StringToAddList[i].numberOfPieces + ",\"\",\"\",0," + StringToAddList[i].idRecord + "," + StringToAddList[i].idProduct;
                    appendTextFTP += "\r\n\"" + StringToAddList[i].codeOfOrder + "\",\"" + StringToAddList[i].nameProduct + "\"," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + StringToAddList[i].numberOfPieces + ",\"\",\"\",0";
                }
                else
                {
                    string[] item = index.ToString().Split(",");
                    if (item[8] != StringToAddList[i].idRecord || item[9] != StringToAddList[i].idProduct)
                    {
                        Random rand = new Random();
                        appendTextLocal += "\r\n\"" + StringToAddList[i].codeOfOrder + "\",\"" + StringToAddList[i].nameProduct + "\"," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + StringToAddList[i].numberOfPieces + ",\"\",\"\",0," + StringToAddList[i].idRecord + "," + StringToAddList[i].idProduct;
                        appendTextFTP += "\r\n\"" + StringToAddList[i].codeOfOrder + "\",\"" + StringToAddList[i].nameProduct + "\"," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + StringToAddList[i].numberOfPieces + ",\"\",\"\",0";
                    }
                    else
                    {
                        // Controllo se il codice della commessa o la quantità del prodotto è stata modificata
                        string modifyString = "";
                        string newcode = StringToAddList[i].codeOfOrder;
                        string newQuantity = StringToAddList[i].numberOfPieces;
                        bool modify = false;

                        Random rand = new Random();

                        item[0] = item[0].ToString().Replace("\"", "");
                        if (item[0] != StringToAddList[i].codeOfOrder)
                        {
                            newcode = StringToAddList[i].codeOfOrder;
                            modify = true;
                        }
                        if (item[4] != StringToAddList[i].numberOfPieces)
                        {
                            newQuantity += StringToAddList[i].numberOfPieces;
                            modify = true;
                        }

                        if (modify)
                        {
                            modifyString = "\"" + newcode + "\",\"" + StringToAddList[i].nameProduct + "\"," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + (float)(Math.Round(rand.NextDouble() * 100f) / 100f) + "," + newQuantity + ",\"\",\"\",0," + StringToAddList[i].idRecord + "," + StringToAddList[i].idProduct;
                            var id = jobCopyList.FindIndex(x => x.Contains(StringToAddList[i].idRecord + "," + StringToAddList[i].idProduct));

                            printTextToConsole("Modifica a commessa già inserita rilevata");
                            updateRowOfLocalJobFile(id, modifyString);
                            updateFTPJobFileFromLocalJobFile();
                        }
                    }
                }
            }

            if (appendTextLocal != "")
            {
                // Avviso l'utente tramite il terminale
                printTextToConsole("Trovate nuove commesse inserite nel db");
                // Avviso l'utente tramite il terminale
                printTextToConsole("Appendo nel file FTP le nuove commesse");

                // Apro il file in locale
                using (StreamWriter sw = File.AppendText(localfilePath))
                {
                    // Appendo il testo nel file in locale
                    sw.Write(appendTextLocal);
                }

                // Variabili per connessione e operazioni su spazio FTP
                FtpWebRequest request;
                FtpWebResponse response;
                // Imposto l'indirizzo del server e del file FTP
                request = (FtpWebRequest)WebRequest.Create("ftp://79.10.111.184/disk3/job.txt");
                // Specifico il tipo di operazione da fare sul file
                request.Method = WebRequestMethods.Ftp.AppendFile;
                // Converto in byte il testo da appendere
                byte[] fileContents = System.Text.Encoding.UTF8.GetBytes(appendTextFTP);
                // Specifico la lunghezza del testo
                request.ContentLength = fileContents.Length;
                // Specifico utente e password per effettuare l'operazione
                request.Credentials = new NetworkCredential("stslab_test3", "zy'@7FP#");
                // Imposto l'oggetto per ricevere lo status dal server
                Stream requestStream = request.GetRequestStream();
                // Scrivo il testo da appendere nell'oggetto che rappresent l'operazione
                requestStream.Write(fileContents, 0, fileContents.Length);
                // Chiudo l'oggetto
                requestStream.Close();
                // Eseguo la richiesta di operazione e mi salvo la risposta
                response = (FtpWebResponse)request.GetResponse();
                // Avviso l'utente tramite il terminale del risultato dell'operazione
                printTextToConsole("Codice ritornato dal server -> " + response.StatusDescription.ToString().Replace("\n", ""));
                // Chiudo l'oggetto di risposta
                response.Close();
            }
            else
            {
                // Avviso l'utente tramite il terminale
                printTextToConsole("Nessuna nuova commessa trovata nel db");
            }
        }

        public void checkFTPFileTrigger()
        {
            // Avviso l'utente tramite il terminale
            printTextToConsole("Controllo se ci sono modifiche fatte alle commesse nel file FTP");

            try
            {
                // Variabili per connessione e operazioni su spazio FTP
                FtpWebRequest request;
                FtpWebResponse response;
                // Imposto l'indirizzo del server e del file FTP
                request = (FtpWebRequest)WebRequest.Create("ftp://79.10.111.184/disk3/job.txt");
                // Specifico il tipo di operazione da fare sul file
                request.Method = WebRequestMethods.Ftp.DownloadFile;
                // Specifico utente e password per effettuare l'operazione
                request.Credentials = new NetworkCredential("stslab_test3", "zy'@7FP#");
                // Eseguo la richiesta di operazione e mi salvo la risposta
                response = (FtpWebResponse)request.GetResponse();
                // Imposto uno stream per il response
                Stream responseStream = response.GetResponseStream();
                // Imposto un reader per il file
                StreamReader reader = new StreamReader(responseStream);

                // Dichiaro una lista dove andrò a salvare tutte le righe del file FTP
                List<String> jobList = new List<string>();
                try
                {
                    while (!reader.EndOfStream)
                    {
                        jobList.Add(reader.ReadLine());
                    }
                }
                catch
                {

                }
                reader.Close();
                response.Close();


                string localfilePath = Path.Combine(Directory.GetParent(System.IO.Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "STSLabTestGuarnieri/jobCopy.txt");
                string[] text = System.IO.File.ReadAllLines(localfilePath);

                List<string> jobCopyList = new List<string>();

                for (int i = 0; i < text.Length; i++)
                {
                    jobCopyList.Add(text[i]);
                }

                int modifyCounter = 0;
                for (int i = 0; i < jobList.Count; i++)
                {
                    if (jobCopyList[i].ToString() == "") break;
                    string jcl = jobCopyList[i].ToString();
                    string[] jcsplitted = jcl.Split(",");
                    string jl = jobList[i].ToString();
                    string[] jsplitted = jl.Split(",");

                    if (jsplitted[5] != "\"\"" && jsplitted[6] != "\"\"" && jsplitted[7] != "0")
                    {
                        if (jcsplitted[5] == "\"\"" && jcsplitted[6] == "\"\"" && jcsplitted[7] == "0")
                        {
                            // Avviso l'utente tramite il terminale
                            printTextToConsole("Modifica commesse rilevata -> " + jl);
                            // chiamo la funzione per eseguire l'insermineto nel db
                            insertRecordInImTopicsTable(jcsplitted[8], jcsplitted[9], jsplitted[7]);

                            // Aggiorno il file locale modificando la commessa rilevata
                            string[] arrLine = File.ReadAllLines(localfilePath);
                            arrLine[i] = jl + "," + jcsplitted[8] + "," + jcsplitted[9];
                            File.WriteAllLines(localfilePath, arrLine);

                            // Incremento il contatore
                            modifyCounter++;
                        }
                    }
                }

                // Avviso l'utente tramite il terminale
                printTextToConsole("Modifiche totali rilevate -> " + modifyCounter.ToString());
            }
            catch (Exception ex)
            {
                printTextToConsole("Errore -> " + ex.Message);
            }

        }

        // Funzione che rappresenta un ciclo del test
        // Nello specifico:
        // 1. Apre la connessione con il db
        // 2. Scarica le commesse dal db ed controlla se sono state iniserite di nuove in base al contenuto del file locale jobcopy.txt. Se sono state aggiunte nuove commesse le aggiunge nel file FTP
        // 3. Controlla se nel file FTP sono state modificate le commesse, se si aggiorna il file locale e i inserisce un nuovo record nel db
        // 4. Chiude la connessione con il db
        public void cycle()
        {
            // Se si riesce ad aprire la connessione allora si esegue il ciclo
            if (openConnection() == 1)
            {
                returnOrders();
                checkJobCopyTextFile();
                checkFTPFileTrigger();
                closeConnection();
            }

        }

        static void Main(string[] args)
        {
            Program p = new Program();
            int cycleCounter = 1;
            DateTime start, end;

            Console.WriteLine("Per uscire dal loop cliccare sul terminale e premere ESC");
            do
            {
                while (!Console.KeyAvailable)
                {
                    // Mi salvo il datetime di inizio ciclo
                    start = DateTime.Now;

                    // Avviso l'utente tramite il terminale che inizia il ciclo
                    p.printTextToConsole("INIZIO CICLO " + cycleCounter.ToString());
                    // Eseguo la funzione che rappresenta un ciclo intero
                    p.cycle();

                    // Mi salvo il datetime di fine ciclo
                    end = DateTime.Now;
                    // Calcolo la differenza
                    var min = (end - start).Minutes;
                    var sec = (end - start).Seconds;
                    var mil = (end - start).Milliseconds;

                    // Avviso l'utente tramite il terminale che il ciclo è finito e scrivo la durata nel formato min:sec:millisec
                    p.printTextToConsole("FINE CICLO " + cycleCounter.ToString() + ", durata min: " + min.ToString() + " sec: " + sec.ToString() + " millisec: " + mil.ToString() + "\n");

                    // Incremento il contatore dei cicli
                    cycleCounter++;

                    // Aspetto 500ms prima di ripetere il ciclo
                    System.Threading.Thread.Sleep(500);
                }
            } while (Console.ReadKey(true).Key != ConsoleKey.Escape);
        }
    }
}
