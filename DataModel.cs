using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using MySql.Data.MySqlClient;

namespace _DataModel
{
    class DataModel
    {
        public string newModel;
        public string oldModel;

        public DataModel()
        {
            newModel = Dataport_TCP_LoadSalesOrdes.Properties.Resources.DB_CON_NEWMODEL;
            oldModel = Dataport_TCP_LoadSalesOrdes.Properties.Resources.DB_CON_OLDMODEL;
        }

        public DataTable getPO_orders_NEW()
        {
            DataTable result = new DataTable();
            MySqlConnection con = new MySqlConnection(newModel);

            string query = @"SELECT DISTINCT order_no FROM sales_orders WHERE processed = 0";
            con.Open();

            MySqlCommand cmd = new MySqlCommand(query, con);
            cmd = new MySqlCommand(query, con);
            cmd.CommandTimeout = 99999999;

            MySqlDataReader dataReader = cmd.ExecuteReader();
            result.Load(dataReader);
            con.Close();

            return result;
        }


        public DataTable getPO_lines_NEW()
        {
            DataTable result = new DataTable();
            MySqlConnection con = new MySqlConnection(newModel);

            string query = @"SELECT 
                                order_no
                                , EAN
                                , ASIN
                                , price
                                , IFNULL(id_product, 0) as id_product
                                , SUM(quantity) as quantity
                                , Unix_timestamp(min_order_date) as min_order_date_unix
                                , Unix_timestamp(max_order_date) as max_order_date_unix
                                , Unix_timestamp(order_date) as order_date_unix
                                , Unix_timestamp(timestamp) as timestamp_unix
                                , vc.vendor_code as cod_prov
                                , so.vendor_id
                                , vw.code as centro_desp
                                , original_file_path
                                , p.name as producto
                            FROM 
                                sales_orders so
                            LEFT JOIN vendor_warehouse vw ON vw.id = so.id_vendor_warehouse 
                            LEFT JOIN vendor_codes vc ON vc.id = so.vendor_id 
                            LEFT JOIN products p ON p.id = so.id_product
                            WHERE 
                                processed = 0 
                            GROUP BY 
                                order_no,EAN, ASIN;";
            con.Open();

            MySqlCommand cmd = new MySqlCommand(query, con);
            cmd = new MySqlCommand(query, con);
            cmd.CommandTimeout = 99999999;

            MySqlDataReader dataReader = cmd.ExecuteReader();
            result.Load(dataReader);
            con.Close();

            return result;
        }

        public long InsertCab_OLD(string query)
        {
            MySqlConnection con = new MySqlConnection(oldModel);
            con.Open();
            MySqlCommand comm = con.CreateCommand();
            comm.CommandText = query; 
            comm.ExecuteNonQuery();              
            long id = comm.LastInsertedId;
            con.Close();

            return id;

        }
        
        public long InsertDet_OLD(string query)
        {
            MySqlConnection con = new MySqlConnection(oldModel);
            con.Open();
            MySqlCommand comm = con.CreateCommand();
            comm.CommandText = query; 
            comm.ExecuteNonQuery();              
            long id = comm.LastInsertedId;
            con.Close();

            return id;

        }


        public DataTable getPO_orders_OLD()
        {
            DataTable result = new DataTable();
            MySqlConnection con = new MySqlConnection(oldModel);

            string query = @"SELECT DISTINCT num_pedido FROM amazon_pedidos_vendor_cab";
            con.Open();

            MySqlCommand cmd = new MySqlCommand(query, con);
            cmd = new MySqlCommand(query, con);
            cmd.CommandTimeout = 99999999;

            MySqlDataReader dataReader = cmd.ExecuteReader();
            result.Load(dataReader);
            con.Close();

            return result;
        }

        public int getLast_POid()
        {
            DataTable result = new DataTable();
            int id = 0;
            MySqlConnection con = new MySqlConnection(oldModel);

            string query = @"SELECT id FROM amazon_pedidos_vendor_cab ORDER BY id DESC LIMIT 1";
            con.Open();

            MySqlCommand cmd = new MySqlCommand(query, con);
            cmd = new MySqlCommand(query, con);
            cmd.CommandTimeout = 99999999;

            MySqlDataReader dataReader = cmd.ExecuteReader();
            result.Load(dataReader);
            con.Close();


            if (result.Rows.Count > 0)
            {
                id = Convert.ToInt32(result.Rows[0]["id"].ToString());
            }
            else
            {
                id = 0;
            }
            

            return id;
        }


        public void InsertBulkPO(List<string> order)
        {

            var result = String.Join("\n", order.ToArray());
            MySqlConnection myConnection = new MySqlConnection(oldModel);
            myConnection.Open();

            MySqlCommand myCommand = myConnection.CreateCommand();
            MySqlTransaction myTrans;

            myTrans = myConnection.BeginTransaction();

            myCommand.Connection = myConnection;
            myCommand.Transaction = myTrans;

            try
            {
                foreach(string query in order)
                {
                    myCommand.CommandText = query;
                    myCommand.ExecuteNonQuery();
                }
                myTrans.Commit();
            }
            catch (Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (MySqlException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("MySqlException: " + ex.GetType() +"; rolling back transaction.");
                    }
                }
                Console.WriteLine("Exception: " + e.GetType() + "; rolling back transaction.");
            }
            finally
            {
                myConnection.Close();
            }
        }


    }
}
