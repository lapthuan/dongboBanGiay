require("dotenv").config();
const { sqlPool, connectSQL } = require("../connect/connect_sqlserver");
const { connectMysql, mysqlConnection } = require("../connect/connect_mysql");
const { connectOracle, executeOracleQuery } = require("../connect/connect_oracle");

const dayjs = require("dayjs");

connectSQL();
connectMysql();
connectOracle();

function myFunction() {

  mysqlConnection.query(`SELECT nv.*
  FROM nhanvien nv
  JOIN cuahang ch ON ch.MaCuaHang = nv.MaCuaHang
  JOIN chinhanh cn ON ch.MaCN = cn.MaCN
  WHERE cn.DiaChi = 'Vĩnh Long'`, (selectErr, results) => {

    results.forEach(async (row) => {

      const { MaNhanVien, TenNhanVien, GioiTinh, NgaySinh, DiaChi, SDT, Email, MaCuaHang } = row;
      const checkNhanVien = `SELECT COUNT(*) AS COUNT FROM NHANVIEN WHERE MaNhanVien = '${MaNhanVien}'`;
      const sqlCheckResult = await sqlPool.request().query(checkNhanVien);

      if (sqlCheckResult.recordset[0].COUNT == 0) {

        const insertQuery = `INSERT INTO nhanvien VALUES ('${MaNhanVien}', N'${TenNhanVien}', N'${GioiTinh}','${dayjs(NgaySinh).format('YYYY/MM/DD')}',N'${DiaChi}','${SDT}','${Email}','${MaCuaHang}')`;

        sqlPool.request().query(insertQuery, (insertErr) => {
          if (insertErr) {
            console.error('Lỗi thêm dữ liệu vào SQL Server:', insertErr);
          } else {
            console.log(`Dữ liệu với MaNV ${MaNhanVien} đã được thêm vào SQL Server`);
          }
        });
      }

    });

  });
  mysqlConnection.query(`select hd.* from hoadon hd inner join khachhang kh on kh.MaKhachHang = hd.MaKhachHang
  inner join nhanvien nv on nv.MaNhanVien = hd.MaNhanVien
  inner join cuahang ch on ch.MaCuaHang = kh.MaCH
  inner join chinhanh cn on cn.MaCN = ch.MaCN where cn.DiaChi = 'Vĩnh Long'`, async (selectErr, results) => {
    results.forEach(async (row) => {

      const { MaHD, NgayLap, HinhThucTT, MaNhanVien, MaKhachHang } = row;
      const checkMaHD = `SELECT COUNT(*) AS COUNT FROM hoadon WHERE MaHD = '${MaHD}' `;
      const sqlCheckResult = await sqlPool.request().query(checkMaHD);
      const checkNhanVien = `SELECT COUNT(*) AS COUNT FROM NHANVIEN WHERE MaNhanVien = '${MaNhanVien}'`;
      const sqlCheckResult2 = await sqlPool.request().query(checkNhanVien);
      const checkKhachHang = `SELECT COUNT(*) AS COUNT FROM KHACHHANG WHERE MaKhachHang = '${MaKhachHang}'`;
      const sqlCheckResult3 = await sqlPool.request().query(checkKhachHang);

      if (sqlCheckResult3.recordset[0].COUNT > 0) {
        if (sqlCheckResult2.recordset[0].COUNT > 0) {
          if (sqlCheckResult.recordset[0].COUNT == 0) {

            const insertQuery = `INSERT INTO hoadon VALUES ('${MaHD}', '${dayjs(NgayLap).format('YYYY/MM/DD')}', N'${HinhThucTT}', '${MaNhanVien}', '${MaKhachHang}')`;

            sqlPool.request().query(insertQuery, (insertErr) => {
              if (insertErr) {
                console.error('Lỗi thêm dữ liệu vào SQL Server:', insertErr);
              } else {
                console.log(`Dữ liệu với MaHD ${MaHD} đã được thêm vào SQL Server`);
              }
            });
          }
        } else {
          console.log('SqlServer: nhân viên không tồn tại')
        }
      } else {
        console.log('SqlServer: khách hàng không tồn tại')
      }
    });

  });

  mysqlConnection.query(`SELECT kh.*
  FROM khachhang kh
  JOIN cuahang ch ON ch.MaCuaHang = kh.MaCH
  JOIN chinhanh cn ON ch.MaCN = cn.MaCN
  WHERE cn.DiaChi = 'Vĩnh Long'`, async (selectErr, results) => {
    results.forEach(async (row) => {

      const { MaKhachHang, TenKhachHang, DiaChi, SDT, MaCH } = row;
      const checkKhachHang = `SELECT COUNT(*) AS COUNT FROM KHACHHANG WHERE MaKhachHang = '${MaKhachHang}'`;
      const sqlCheckResult = await sqlPool.request().query(checkKhachHang);

      if (sqlCheckResult.recordset[0].COUNT == 0) {

        const insertQuery = `INSERT INTO khachhang VALUES ('${MaKhachHang}', N'${TenKhachHang}', N'${DiaChi}', '${SDT}', '${MaCH}')`;

        sqlPool.request().query(insertQuery, (insertErr) => {
          if (insertErr) {
            console.error('Lỗi thêm dữ liệu vào SQL Server:', insertErr);
          } else {
            console.log(`Dữ liệu với MaKH ${MaKhachHang} đã được thêm vào SQL Server`);
          }
        });
      }

    });

  });


  //Oracel 
  mysqlConnection.query(`SELECT * FROM nhanvien except SELECT nv.*
  FROM nhanvien nv
  JOIN cuahang ch ON ch.MaCuaHang = nv.MaCuaHang
  JOIN chinhanh cn ON ch.MaCN = cn.MaCN
  WHERE cn.DiaChi = 'Vĩnh Long'`, (selectErr, results) => {
    results.forEach(async (row) => {
      const { MaNhanVien, TenNhanVien, GioiTinh, NgaySinh, DiaChi, SDT, Email, MaCuaHang } = row;
      const checkNhanVien = await executeOracleQuery(
        `SELECT COUNT(*) AS COUNT FROM NHANVIEN WHERE MaNhanVien = :MaNhanVien`,
        [MaNhanVien]
      );


      if (checkNhanVien.rows[0][0] == 0) {

        console.log('row', row)
        const insertQuery = "INSERT INTO nhanvien (MaNhanVien, TenNhanVien, GioiTinh, NgaySinh, DiaChi, SDT, Email, MaCuaHang) VALUES (:1, :2,:3,TO_DATE(:4, 'yyyy-mm-dd'),:5,:6,:7,:8)";

        const resultsOracel = await executeOracleQuery(insertQuery, [
          MaNhanVien, TenNhanVien, GioiTinh, dayjs(NgaySinh).format('YYYY/MM/DD'), DiaChi, SDT, Email, MaCuaHang
        ]);
        console.log('resultsOracel', resultsOracel)
      }
    });

  });

  mysqlConnection.query(`SELECT * FROM khachhang except SELECT kh.*
  FROM khachhang kh
  JOIN cuahang ch ON ch.MaCuaHang = kh.MaCH
  JOIN chinhanh cn ON ch.MaCN = cn.MaCN
  WHERE cn.DiaChi = 'Vĩnh Long'
  `, (selectErr, results) => {

    results.forEach(async (row) => {
      const { MaKhachHang, TenKhachHang, DiaChi, SDT, MaCH } = row;
      const checkKhachHang = await executeOracleQuery(
        `SELECT COUNT(*) AS COUNT FROM khachhang WHERE MaKhachHang = :MaKhachHang`,
        [MaKhachHang]
      );

      if (checkKhachHang.rows[0][0] == 0) {
        console.log('row', row)
        const insertQuery = "INSERT INTO khachhang (MaKhachHang, TenKhachHang, DiaChi, SDT, MaCH) VALUES (:1, :2, :3, :4, :5)";

        const resultsOracle = await executeOracleQuery(insertQuery, [
          MaKhachHang, TenKhachHang, DiaChi, SDT, MaCH
        ]);
        console.log('resultsOracle', resultsOracle)
      }
    });
  });
  mysqlConnection.query(`SELECT * FROM hoadon except select hd.* from hoadon hd inner join khachhang kh on kh.MaKhachHang = hd.MaKhachHang
inner join nhanvien nv on nv.MaNhanVien = hd.MaNhanVien
inner join cuahang ch on ch.MaCuaHang = kh.MaCH
inner join chinhanh cn on cn.MaCN = ch.MaCN where cn.DiaChi = 'Vĩnh Long'
  `, (selectErr, results) => {

    results.forEach(async (row) => {
      const { MaHD, NgayLap, HinhThucTT, MaNhanVien, MaKhachHang } = row;
      const checkMaHD = await executeOracleQuery(
        `SELECT COUNT(*) AS COUNT FROM hoadon WHERE MaHD = :MaHD`,
        [MaHD]
      );
      const checkNhanVien = await executeOracleQuery(
        `SELECT COUNT(*) AS COUNT FROM NHANVIEN WHERE MaNhanVien = :MaNhanVien`,
        [MaNhanVien]
      );
      const checkKhachHang = await executeOracleQuery(
        `SELECT COUNT(*) AS COUNT FROM khachhang WHERE MaKhachHang = :MaKhachHang`,
        [MaKhachHang]
      );
      if (checkKhachHang.rows[0][0] > 0) {
        if (checkNhanVien.rows[0][0] > 0) {
          if (checkMaHD.rows[0][0] == 0) {

            const insertQuery = "INSERT INTO hoadon (MaHD, NgayLap, HinhThucTT, MaNhanVien, MaKhachHang ) VALUES (:1,TO_DATE(:2, 'yyyy-mm-dd'), :3, :4, :5)";

            const resultsOracle = await executeOracleQuery(insertQuery, [
              MaHD, dayjs(NgayLap).format('YYYY/MM/DD'), HinhThucTT, MaNhanVien, MaKhachHang
            ]);
            console.log('resultsOracle', resultsOracle)
          }
        } else {


          console.log('Oracel: Nhân viên không tồn tại')
        }
      } else {
        console.log('Oracel: khách hàng không tồn tại')
      }
    });
  });

}
setInterval(myFunction, 10000);
