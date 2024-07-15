# Perioperative Multitask Risk Alert Toolkit (PM-RAT)

## Introduction
The project is designed as a comprehensive toolkit for perioperative care, spanning preoperative, intraoperative, and postoperative phases. This toolkit integrates multiple deep learning models, each tailored to specific tasks within each phase of the surgical process. The primary goal is to enhance patient safety and outcomes through timely and accurate alerts.

## Project Structure

1. The `deep_learning` folder contains code for defining model structures for different stages and tasks, as well as code for training and testing these tasks. The `PM_RAT_demo.ipynb` file provides an example of how to use the PM-RAT toolkit for perioperative multitask risk alerting and management.
2. The `derived_table` folder includes preprocessing for the INSPIRE database, which involves generating new variables such as BMI and VIS that were not originally in the INSPIRE database. Scripts like `preop_stats.R`, `icd_outcome.R`, `med_outcome.R`, `lab_outcome.R`, and `ward_vital_outcome.R` define preoperative comorbidities and postoperative complications. The `scoring.R` script calculates various preoperative scores, and the `NSQIP.R` script samples some cases to prepare for online calculation of NSQIP scores.
3. The `EMR_LIP.R` script provides all the function tools used for data preprocessing, and the `data_extract.R` script offers code for generating data in a "patient-centered" manner.
