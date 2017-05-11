import mongoose from 'mongoose';

const Schema = mongoose.Schema;

const Record = new Schema({
  location: {
    lat: Number,
    lng: Number,
  },
  education: String,
  salary: Number,
  experience: Number,
  department: String,
  patientNurseRatio: Number,
  createdAt: Date,
});

export default mongoose.model('records', Record);

/*
// Example record
{
  location: {
    lat: 42.2808,
    lng: 83.7430,
  },
  education: 'Bachelors',
  salary: 30, // per hour
  experience: 4.5, //years
  department: 'Oncology Nurse',
  patientNurseRatio: 4.7, // 1 is always the nurse
}
*/
