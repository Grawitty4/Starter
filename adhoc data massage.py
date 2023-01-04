# Databricks notebook source
import pandas as pd

def ordertagging(row):
  if row["cumsumqty"] < order:
    val = order
  elif (row["cumsumqty"] >= order):
    if row["cumsumlagqty"] < order:
      val = order
    else:
      val = 0
  return val

data = [[1,10],[10,8],[1000,9],[500,5]]
order = 1000

tiles = pd.DataFrame(data,columns=["Qty","Price"])

tiles["cumsumqty"] = tiles.Qty.cumsum().astype(int)
tiles["cumsumlagqty"] = tiles["cumsumqty"].shift(1).fillna(0).astype(int)
tiles["flag"] = tiles.apply(ordertagging,axis=1)
tiles["Residual"] = tiles['flag']-tiles['cumsumlagqty']
tiles["Orders"] = tiles.apply(lambda x : x['Qty'] if x['Residual'] > x['Qty'] else x['Residual'],axis=1)
tiles["Orders"] = tiles.apply(lambda x : x['Orders'] if x['flag']!=0 else 0,axis=1)
tiles["Inventory"] = tiles['Qty']-tiles['Orders']
tiles["Payable"] = tiles['Orders']*tiles['Price']
tiles["PayableCumulative"] = tiles.Payable.cumsum().astype(int)
tiles.drop(['cumsumqty','cumsumlagqty','flag','Residual'],axis=1,inplace=True)

tiles
