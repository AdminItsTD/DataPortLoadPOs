using System;
using System.Data;
using System.Collections.Generic;
using System.Threading;
using Spectre.Console;
using _DataModel;

namespace Dataport_TCP_LoadPurchaseOrders
{
    class Program
    {
        static void Main(string[] args)
        {
            //Console.WriteLine("DATAPORT PANEL DATA");
            AnsiConsole.MarkupLine("[invert]DATAPORT PANEL DATA  \n[darkcyan]PURCHASE ORDERS TRANSFER[/][/]\n\n");
            AnsiConsole.MarkupLine($"Process Start at:[bold green] {DateTime.Now.ToString()}[/]");

            DataModel getData = new DataModel();

            DataTable new_Pos = getData.getPO_orders_NEW();
            DataTable new_Lines = getData.getPO_lines_NEW();
            DataTable old_Pos = getData.getPO_orders_OLD();

            int cab_count = 0;
            int det_count = 0;

            DateTime inicio = DateTime.Now;

            var table = new Table();
            table.AddColumn("New Model Records");
            table.AddColumn("Old Model Records");
            table.AddRow($"[blue]{new_Pos.Rows.Count}[/]", $"[green]{old_Pos.Rows.Count}[/]");
            AnsiConsole.Write(table);

            int id_cab = getData.getLast_POid();

            if(new_Pos.Rows.Count > 0)
            {
                foreach(DataRow row in new_Pos.Rows)
                {
                    DataRow[] dt_old_exists = old_Pos.Select("num_pedido = '" + row["order_no"] + "'");

                    if(dt_old_exists.Length == 0)
                    {
                        
                        DataRow[] dt_new_lines = new_Lines.Select("order_no = '" + row["order_no"] + "'");

                        if(dt_new_lines.Length > 0)
                        {
                            List<string> order_queries = new List<string>();

                            string order_no     = dt_new_lines[0]["order_no"].ToString();
                            string cod_prov     = dt_new_lines[0]["cod_prov"].ToString();
                            string centro_desp  = dt_new_lines[0]["centro_desp"].ToString();
                            string fichero      = dt_new_lines[0]["original_file_path"].ToString();
                            string ubicacion    = "''";

                            int fecha_minima    = Convert.ToInt32(dt_new_lines[0]["min_order_date_unix"]);
                            int fecha_maxima    = Convert.ToInt32(dt_new_lines[0]["max_order_date_unix"]);
                            int fecha_prev      = Convert.ToInt32(dt_new_lines[0]["order_date_unix"]);
                            int id_vendor       = Convert.ToInt32(dt_new_lines[0]["vendor_id"]);                           
                            int fecha_ped       = Convert.ToInt32(dt_new_lines[0]["order_date_unix"]);
                            int procesado       = 0;
                            int proc_automatico = 0;
                            int fecha_proc      = 0;                           
                            int fecha_ingreso   = Convert.ToInt32(dt_new_lines[0]["timestamp_unix"]);
                            id_cab++;
                            cab_count++;

                            string query_cab = @"INSERT INTO amazon_pedidos_vendor_cab(id,id_entity,num_pedido,fecha_minima,fecha_maxima,fecha_prev,cod_prov,id_vendor,centro_desp,fecha_ped,procesado,proc_automatico,fecha_proc,fichero,fecha_ingreso,ubicacion) VALUES ("+id_cab+",1,'" + order_no + "',"+ fecha_minima + "," + fecha_maxima + "," + fecha_prev + ",'" + cod_prov + "'," + id_vendor + ",'" + centro_desp + "'," + fecha_ped + "," + procesado + "," + proc_automatico + "," + fecha_proc + ",'" + fichero + "'," + fecha_ingreso + "," + ubicacion + ")";
                            order_queries.Add(query_cab);

                            var root = new Tree($"{order_no}");

                            foreach (DataRow new_row in dt_new_lines)
                            {
                                root.AddNode($"[blue]{new_row["EAN"]}[/] - {new_row["quantity"]} UDS");

                                string modelo = "";
                                string asin = new_row["ASIN"].ToString();
                                string ean = new_row["EAN"].ToString();
                                string producto = new_row["producto"].ToString();
                                decimal coste = Convert.ToDecimal(new_row["price"].ToString());
                                int cantidad = Convert.ToInt32(new_row["quantity"].ToString());
                                string moneda = "EUR";
                                int disponible = 3;
                                int cantidad_acep = 0;
                                int procesado_ = 0;
                                int duplicado = 0;
                                int finalizado = 0;
                                int verificado = 0;
                                string observ = "";
                                int id_product = Convert.ToInt32(new_row["id_product"].ToString());
                                int fecha_verificado =0;
                                det_count++;


                                string query_det = @"INSERT INTO amazon_pedidos_vendor_det (id_cab, id_externo, modelo, asin,producto, coste, cantidad, moneda, disponible, cantidad_acep, procesado, duplicado, finalizado, verificado, observ, id_product, fecha_verificado) VALUES("+ id_cab + ", '" + ean + "', '" + modelo + "', '" + asin + "', '" + producto + "', " + coste.ToString().Replace(',', '.') + ",  " + cantidad + ", '"+ moneda + "', '" + disponible + "', '" + cantidad_acep + "', '" + procesado_ + "', '" + duplicado + "', '" + finalizado + "', '" + verificado + "', '" + observ + "', '" + id_product + "', '" + fecha_verificado + "')";


                                order_queries.Add(query_det);

                            }
                            AnsiConsole.Write(root);

                            //string uprocess = "UPDATE amazon_pedidos_vendor_cab SET procesado = 1";
                            //order_queries.Add(uprocess);
                            //string upd_process = "UPDATE sales_orders SET processed = 1";
                            //order_queries.Add(upd_process);


                            getData.InsertBulkPO(order_queries);

                        }

                    }

                }

            }
            DateTime fin = DateTime.Now;
            TimeSpan total = fin - inicio;

            AnsiConsole.MarkupLine($"\n" +
                $"Process Ends at:[bold red] {DateTime.Now.ToString()}[/]");
            AnsiConsole.MarkupLine($"Total Execution:[bold red] {total.TotalSeconds}[/]\n");

            if (cab_count > 0 && det_count > 0)
            {
                AnsiConsole.Write(new BarChart()
                   .Width(100)
                   .Label("[invert darkcyan]RESUME[/]\n")
                   .CenterLabel()
                   .AddItem("Inserted Cabs", cab_count, Color.Blue)
                   .AddItem("Inserted Dets", det_count, Color.Green)
               );
            }
            else
            {
                AnsiConsole.MarkupLine("[invert darkcyan]RESUME[/]\n");
                AnsiConsole.MarkupLine("[bold]Nothing inserted[/]\n");
            }
           

            Thread.Sleep(10000);
        }

    }

}
