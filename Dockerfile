FROM php:7.3-rc-fpm

RUN apt-get update

RUN apt-get install -y nano curl zip unzip git

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer

RUN mkdir /app

WORKDIR /app

CMD [ "php-fpm" ]
