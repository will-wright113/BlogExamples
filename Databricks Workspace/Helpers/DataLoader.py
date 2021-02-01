# Databricks notebook source
def data_loader(Location, Format):
  
  if Location == None or Location == "":
    raise Exception("A Location must be included")
  if Format == None or Format == "":
    raise Exception("A Format must be included")
  
  df = spark.read.format(Format).options(header='true', inferSchema='true').load(Location)
    
  return(df)
