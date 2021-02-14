mdbook build
pushd book
rm -f ballista-book.tgz
tar czf ballista-book.tgz *
scp -i ~/.ssh/id_rsa_lightsail.pem ballista-book.tgz ubuntu@18.188.179.245:
ssh -i ~/.ssh/id_rsa_lightsail.pem ubuntu@18.188.179.245 "cd /var/www/ballistacompute.org/docs ; tar xzf /home/ubuntu/ballista-book.tgz"
popd
